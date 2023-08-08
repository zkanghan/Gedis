package main

type Bitmap []byte

func (b *Bitmap) BitLength() int64 {
	return b.ByteLength() << 3
}

func (b *Bitmap) ByteLength() int64 {
	return int64(len(*b))
}

func growIfNeedBitmap(b *Bitmap, bitLen int64) *Bitmap {
	byteLen := bitLen >> 3
	if b == nil {
		bm := make(Bitmap, byteLen+1)
		return &bm
	}

	if int64(len(*b)) < byteLen+1 {
		*b = append(*b, make(Bitmap, byteLen+1-int64(len(*b)))...)
	}
	return b
}

func (b *Bitmap) SetBit(bitOffset int64, on int64) {
	growIfNeedBitmap(b, bitOffset)

	/* Get current values */
	offset := bitOffset >> 3
	byteVal := (*b)[offset]
	bit := 7 - (bitOffset & 0x7)
	bitVal := byte(byteVal & (1 << bit))

	/* Either it is newly created, changed length, or the bit changes before and after.
	 * Note that the bitval here is actually a decimal number.
	 * So we need to use `!!` to convert it to 0 or 1 for comparison. */
	if ^^bitVal != byte(on) {
		byteVal &= ^(1 << bit)
		byteVal |= (byte(on) & 0x1) << bit
		(*b)[offset] = byteVal
	}

}

func (b *Bitmap) GetBit(bitOffset int64) int {
	if bitOffset > int64(len(*b))<<3 {
		return 0
	}

	offset := bitOffset >> 3
	bit := 7 - (bitOffset & 0x7)
	bitVal := (*b)[offset] & (1 << bit)
	if bitVal > 0 {
		return 1
	} else {
		return 0
	}
}

/* Bit operations. */

var setbitCommand CommandProc = func(c *GedisClient) {
	var bitOffset, on int64

	if GetNumber(c.args[2].StrVal(), &bitOffset) != nil || GetNumber(c.args[3].StrVal(), &on) != nil {
		c.AddReply(REPLY_INVALID_VALUE)
		return
	}

	/* Bits can only be set or cleared */
	if on & ^1 > 0 {
		c.AddReply(REPLY_INVALID_VALUE)
		return
	}

	bobj := LookupKey(c.args[1])
	if bobj != nil && bobj.Type_ != BITMAP {
		c.AddReply(REPLY_WRONG_TYPE)
		return
	}

	var bm *Bitmap
	if bobj == nil {
		bm = growIfNeedBitmap(bm, bitOffset)
		_ = server.db.data.Add(c.args[1], NewObject(BITMAP, bm))
	} else {
		bm = bobj.Val_.(*Bitmap)
	}

	bm.SetBit(bitOffset, on)
	c.AddReplyInt(int(on))
}

var getbitCommand CommandProc = func(c *GedisClient) {
	var bitOffset int64
	if GetNumber(c.args[2].StrVal(), &bitOffset) != nil {
		c.AddReply(REPLY_INVALID_VALUE)
		return
	}

	bobj := LookupKey(c.args[1])
	if bobj == nil {
		c.AddReply(REPLY_NIL)
		return
	}
	if bobj.Type_ != BITMAP {
		c.AddReply(REPLY_WRONG_TYPE)
		return
	}
	bm := bobj.Val_.(*Bitmap)

	c.AddReplyInt(bm.GetBit(bitOffset))
}
