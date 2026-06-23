// Portions adapted from github.com/klauspost/compress/zstd (Apache 2.0).

package zstd

import (
	"errors"
	"fmt"
	"sync"
)

const (
	minEncTableLog = 5
	maxEncTableLog = 8
)

type symbolTransform struct {
	deltaNbBits    uint32
	deltaFindState int16
	outBits        uint8
}

type cTable struct {
	tableSymbol []byte
	stateTable  []uint16
	symbolTT    []symbolTransform
}

type fseEncoder struct {
	symbolLen      uint16
	actualTableLog uint8
	ct             cTable
	norm           [256]int16
	preDefined     bool
}

type cState struct {
	bw         *bitWriter
	stateTable []uint16
	state      uint16
}

var (
	fsePredefEnc [3]fseEncoder
	predefOnce   sync.Once
)

func initPredefinedEncoders() {
	predefOnce.Do(func() {
		norms := [3][]int16{
			{4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1,
				2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1,
				-1, -1, -1, -1},
			{1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1},
			{1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1,
				-1, -1, -1, -1, -1},
		}
		logs := [3]uint8{6, 5, 6}
		lens := [3]uint16{36, 29, 53}
		bitTables := [3][]byte{llBitsTable[:], nil, mlBitsTable[:]}
		for i := range fsePredefEnc {
			enc := &fsePredefEnc[i]
			copy(enc.norm[:], norms[i])
			enc.symbolLen = lens[i]
			enc.actualTableLog = logs[i]
			enc.preDefined = true
			if err := enc.buildCTable(); err != nil {
				panic(fmt.Errorf("zstd: predefined encoder %d: %w", i, err))
			}
			enc.setBits(bitTables[i])
		}
	})
}

func tableStep(tableSize uint32) uint32 {
	return (tableSize >> 1) + (tableSize >> 3) + 3
}

func (s *fseEncoder) allocCTable() {
	tableSize := 1 << s.actualTableLog
	if cap(s.ct.tableSymbol) < tableSize {
		s.ct.tableSymbol = make([]byte, tableSize)
	}
	s.ct.tableSymbol = s.ct.tableSymbol[:tableSize]
	if cap(s.ct.stateTable) < tableSize {
		s.ct.stateTable = make([]uint16, tableSize)
	}
	s.ct.stateTable = s.ct.stateTable[:tableSize]
	if cap(s.ct.symbolTT) < 256 {
		s.ct.symbolTT = make([]symbolTransform, 256)
	}
	s.ct.symbolTT = s.ct.symbolTT[:256]
}

func (s *fseEncoder) buildCTable() error {
	tableSize := uint32(1 << s.actualTableLog)
	highThreshold := tableSize - 1
	var cumul [256]int16

	s.allocCTable()
	tableSymbol := s.ct.tableSymbol[:tableSize]
	cumul[0] = 0
	for ui, v := range s.norm[:s.symbolLen-1] {
		u := byte(ui)
		if v == -1 {
			cumul[u+1] = cumul[u] + 1
			tableSymbol[highThreshold] = u
			highThreshold--
		} else {
			cumul[u+1] = cumul[u] + v
		}
	}
	u := int(s.symbolLen - 1)
	v := s.norm[s.symbolLen-1]
	if v == -1 {
		cumul[u+1] = cumul[u] + 1
		tableSymbol[highThreshold] = byte(u)
		highThreshold--
	} else {
		cumul[u+1] = cumul[u] + v
	}
	if uint32(cumul[s.symbolLen]) != tableSize {
		return fmt.Errorf("fse cumul mismatch")
	}
	cumul[s.symbolLen] = int16(tableSize) + 1

	step := tableStep(tableSize)
	tableMask := tableSize - 1
	var position uint32
	for ui, v := range s.norm[:s.symbolLen] {
		symbol := byte(ui)
		for range v {
			tableSymbol[position] = symbol
			position = (position + step) & tableMask
			for position > highThreshold {
				position = (position + step) & tableMask
			}
		}
	}
	if position != 0 {
		return errors.New("fse position error")
	}

	table := s.ct.stateTable
	tsi := int(tableSize)
	for u, v := range tableSymbol {
		table[cumul[v]] = uint16(tsi + u)
		cumul[v]++
	}

	symbolTT := s.ct.symbolTT[:s.symbolLen]
	tableLog := s.actualTableLog
	tl := (uint32(tableLog) << 16) - (1 << tableLog)
	var total int16
	for i, v := range s.norm[:s.symbolLen] {
		switch v {
		case 0:
		case -1, 1:
			symbolTT[i].deltaNbBits = tl
			symbolTT[i].deltaFindState = total - 1
			total++
		default:
			maxBitsOut := uint32(tableLog) - highBit32(uint32(v-1))
			minStatePlus := uint32(v) << maxBitsOut
			symbolTT[i].deltaNbBits = (maxBitsOut << 16) - minStatePlus
			symbolTT[i].deltaFindState = total - v
			total += v
		}
	}
	if total != int16(tableSize) {
		return fmt.Errorf("fse total mismatch")
	}
	return nil
}

func (s *fseEncoder) setBits(transform []byte) {
	if transform == nil {
		for i := range s.ct.symbolTT[:s.symbolLen] {
			s.ct.symbolTT[i].outBits = uint8(i)
		}
		return
	}
	for i, v := range transform[:s.symbolLen] {
		s.ct.symbolTT[i].outBits = v
	}
}

func (c *cState) init(bw *bitWriter, ct *cTable, first symbolTransform) {
	c.bw = bw
	c.stateTable = ct.stateTable
	nbBitsOut := (first.deltaNbBits + (1 << 15)) >> 16
	im := int32((nbBitsOut << 16) - first.deltaNbBits)
	lu := (im >> nbBitsOut) + int32(first.deltaFindState)
	c.state = c.stateTable[lu]
}

func (c *cState) flush(tableLog uint8) {
	c.bw.flush32()
	c.bw.addBits16NC(c.state, tableLog)
}

func encodeSequenceStates(wr *bitWriter, ll, of, ml *cState, llEnc, ofEnc, mlEnc *fseEncoder, s seq) {
	llTT := llEnc.ct.symbolTT[:256]
	ofTT := ofEnc.ct.symbolTT[:256]
	mlTT := mlEnc.ct.symbolTT[:256]

	ofB := ofTT[s.ofCode]
	wr.flush32()
	nbBitsOut := (uint32(of.state) + ofB.deltaNbBits) >> 16
	dstState := int32(of.state>>(nbBitsOut&15)) + int32(ofB.deltaFindState)
	wr.addBits16NC(of.state, uint8(nbBitsOut))
	of.state = of.stateTable[dstState]

	outBits := ofB.outBits & 31
	extraBits := uint64(s.offset & bitMask32[outBits])
	extraBitsN := outBits

	mlB := mlTT[s.mlCode]
	nbBitsOut = (uint32(ml.state) + mlB.deltaNbBits) >> 16
	dstState = int32(ml.state>>(nbBitsOut&15)) + int32(mlB.deltaFindState)
	wr.addBits16NC(ml.state, uint8(nbBitsOut))
	ml.state = ml.stateTable[dstState]

	outBits = mlB.outBits & 31
	extraBits = extraBits<<outBits | uint64(s.matchLen&bitMask32[outBits])
	extraBitsN += outBits

	llB := llTT[s.llCode]
	nbBitsOut = (uint32(ll.state) + llB.deltaNbBits) >> 16
	dstState = int32(ll.state>>(nbBitsOut&15)) + int32(llB.deltaFindState)
	wr.addBits16NC(ll.state, uint8(nbBitsOut))
	ll.state = ll.stateTable[dstState]

	outBits = llB.outBits & 31
	extraBits = extraBits<<outBits | uint64(s.litLen&bitMask32[outBits])
	extraBitsN += outBits

	wr.flush32()
	wr.addBits32NC(uint32(extraBits), uint8(extraBitsN))
}

func encodeSequences(out []byte, sequences []seq) []byte {
	initPredefinedEncoders()
	llEnc := &fsePredefEnc[0]
	ofEnc := &fsePredefEnc[1]
	mlEnc := &fsePredefEnc[2]

	switch {
	case len(sequences) < 128:
		out = append(out, uint8(len(sequences)))
	case len(sequences) < 0x7f00:
		n := len(sequences)
		out = append(out, 128+uint8(n>>8), uint8(n))
	default:
		n := len(sequences) - 0x7f00
		out = append(out, 255, uint8(n), uint8(n>>8))
	}
	// All predefined modes.
	out = append(out, 0)

	var wr bitWriter
	wr.reset(out)
	var ll, of, ml cState

	seq := len(sequences) - 1
	s := sequences[seq]
	llEnc.setBits(llBitsTable[:])
	mlEnc.setBits(mlBitsTable[:])
	ofEnc.setBits(nil)

	llB := llEnc.ct.symbolTT[s.llCode]
	ofB := ofEnc.ct.symbolTT[s.ofCode]
	mlB := mlEnc.ct.symbolTT[s.mlCode]
	ll.init(&wr, &llEnc.ct, llB)
	of.init(&wr, &ofEnc.ct, ofB)
	wr.flush32()
	ml.init(&wr, &mlEnc.ct, mlB)

	wr.addBits32NC(s.litLen, llB.outBits)
	wr.addBits32NC(s.matchLen, mlB.outBits)
	wr.flush32()
	wr.addBits32NC(s.offset, ofB.outBits)

	seq--
	for seq >= 0 {
		s = sequences[seq]
		encodeSequenceStates(&wr, &ll, &of, &ml, llEnc, ofEnc, mlEnc, s)
		seq--
	}
	ml.flush(mlEnc.actualTableLog)
	of.flush(ofEnc.actualTableLog)
	ll.flush(llEnc.actualTableLog)
	wr.close()
	return wr.bytes()
}

func appendRawLiteralsHeader(out []byte, litLen int) []byte {
	switch {
	case litLen < 32:
		return append(out, byte(litLen<<3))
	case litLen < 4096:
		return append(out, byte((litLen<<4)&0xf0|0x10), byte(litLen>>4))
	default:
		return append(out,
			byte((litLen<<4)&0xf0|0x30),
			byte(litLen>>4),
			byte(litLen>>12),
		)
	}
}
