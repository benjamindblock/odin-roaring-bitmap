package roaring

import "core:fmt"
import "core:io"
import "core:os"
import "core:encoding/endian"

SERIAL_COOKIE_NO_RUNCONTAINER :: 12346
SERIAL_COOKIE :: 12347
NO_OFFSET_THRESHOLD :: 4

// FORMAT SPEC
// https://github.com/RoaringBitmap/RoaringFormatSpec

Read_Info :: struct {
	has_run_containers: bool,
	container_count: int,
	// *If* we have run containers, this bitset will indicate if each container
	// is a Run_Container (1) or not (0).
	bitset: []byte,
	containers: []Container_Info,
}

Container_Type :: enum {
	Array,
	Bitmap,
	Run,
}

Container_Info :: struct {
	key: u16be,
	type: Container_Type,
	cardinality: int,
	offset: int,
}

header :: proc(r: io.Reader) -> (ri: Read_Info, ok: bool) {
	header: [4]byte
	io.read_at_least(r, header[:], 4)

	cookie, _ := endian.get_u16(header[0:2], .Little)
	fmt.println(header)
	fmt.println(cookie)

	// If we matched the SERIAL_COOKIE_NO_RUNCONTAINER, then the next
	// 4 bytes will determine how many containers there are.
	if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
		ri.has_run_containers = false
		io.read_at_least(r, header[:], 4)
		num, _ := endian.get_u32(header[:], .Little)
		ri.container_count = int(num)
	} else if cookie == SERIAL_COOKIE {
		ri.has_run_containers = true
		num, _ := endian.get_u16(header[2:4], .Little)
		fmt.println("num!", num)
		num += 1
		ri.container_count = int(num)

		bytes_in_bitset := (ri.container_count + 7) / 8

		bitset := make([]byte, bytes_in_bitset)
		io.read_at_least(r, bitset[:], bytes_in_bitset)
		ri.bitset = bitset[:]
	} else {
		return ri, false
	}

	infos := make([]Container_Info, ri.container_count)

	// Get the descriptive headers.
	for i in 0..<ri.container_count {
		io.read_at_least(r, header[:], 4)
		key, _ := endian.get_u16(header[0:2], .Little)

		cardinality, _ := endian.get_u16(header[2:4], .Little)
		cardinality += 1

		type: Container_Type
		if ri.has_run_containers {
			byte_i := i / 8
			// TODO: Confirm this..
			// Swap the indexes around because we treat the bytes as one long bitset, reading it
			// from right (least significant) to left (most significant).
			byte_i = len(ri.bitset) - 1 - byte_i
			bit_i := i - (byte_i * 8)
			bit_is_set := (ri.bitset[byte_i] & (1 << u8(bit_i))) != 0

			if bit_is_set {
				type = .Run
			} else if cardinality <= MAX_ARRAY_LENGTH {
				type = .Array
			} else {
				type = .Bitmap
			}

			// check the bitset!
		} else if cardinality <= MAX_ARRAY_LENGTH {
			type = .Array
		} else {
			type = .Bitmap
		}

		info := Container_Info {
			key = u16be(key),
			cardinality = int(cardinality),
			type = type,
		}

		infos[i] = info
		fmt.println("key", key, "cardinality", cardinality)
	}
	ri.containers = infos

	// Offset header
	// "...then we store (using a 32-bit value) the location (in bytes) of the
	// container from the beginning of the stream (starting with the cookie) for
	// each container."
	// Used for fast random access to a container. Not needed for reading an
	// entire file into a bitmap. That will happen iteratively.
	if !ri.has_run_containers || ri.container_count >= NO_OFFSET_THRESHOLD {
		for i in 0..<ri.container_count {
			io.read_at_least(r, header[:], 4)
			offset, _ := endian.get_u32(header[0:4], .Little)

			cinfo := &ri.containers[i]
			cinfo.offset = int(offset)
			fmt.println("OFFSET", offset)
		}
	}

	rb, _ := init()
	for ci in ri.containers {
		switch ci.type {
		case .Array:
			load_array(r, ci, &rb)
		case .Bitmap:
			load_bitmap(r, ci, &rb)
		case .Run:
			load_run(r, ci, &rb)
		}
	}

	fmt.println("ROARING BITMAP")
	fmt.println(rb)
	fmt.println(to_array(rb))

	return ri, true
}

// "For array containers, we store a sorted list of 16-bit unsigned integer values
// corresponding to the array container. So if there are x values in the array
// container, 2 x bytes are used."
load_array :: proc(r: io.Reader, ci: Container_Info, rb: ^Roaring_Bitmap) {
	ac, _ := array_container_init()

	buffer: [2]byte
	for _ in 0..<ci.cardinality {
		io.read_at_least(r, buffer[:], 2)
		val, _ := endian.get_u16(buffer[0:2], .Little)
		array_container_add(&ac, u16be(val))
	}

	ac.cardinality = array_container_get_cardinality(ac)
	rb.containers[ci.key] = ac
	cindex_ordered_insert(rb, ci.key)
}

// "Bitset containers are stored using exactly 8KB using a bitset serialized with
// 64-bit words. Thus, for example, if value j is present, then word j/64
// (starting at word 0) will have its (j%64) least significant bit set to 1
// (starting at bit 0)."
load_bitmap :: proc(r: io.Reader, ci: Container_Info, rb: ^Roaring_Bitmap) {
	bc, _ := bitmap_container_init()

	buffer: [BYTES_PER_BITMAP]byte
	io.read_at_least(r, buffer[:], BYTES_PER_BITMAP)

	// Read words from left-to-right
	for word_i in 0..<1024 {
		for byte_i in 0..<8 {
			// Read bytes within a word from right-to-left
			reverse_byte_i := 8 - byte_i - 1	

			buffer_i := word_i * 8 + reverse_byte_i
			bc.bitmap[buffer_i] = buffer[buffer_i]
		}
	}

	bc.cardinality = bitmap_container_get_cardinality(bc)
	rb.containers[ci.key] = bc
	cindex_ordered_insert(rb, ci.key)
}

// "A run container is serialized as a 16-bit integer indicating the number of
// runs, followed by a pair of 16-bit values for each run. Runs are
// non-overlapping and sorted. Thus a run container with x runs will use 2 + 4 x
// bytes. Each pair of 16-bit values contains the starting index of the run
// followed by the length of the run minus 1. That is, we interleave values and
// lengths, so that if you have the values 11,12,13,14,15, you store that as 11,4
// where 4 means that beyond 11 itself, there are 4 contiguous values that follow.
// Other example: e.g., 1,10, 20,0, 31,2 would be a concise representation of 1,
// 2, ..., 11, 20, 31, 32, 33"
load_run :: proc(r: io.Reader, ci: Container_Info, rb: ^Roaring_Bitmap) {
	rc, _ := run_container_init()

	buffer: [2]byte
	io.read_at_least(r, buffer[:], 2)
	num_runs, _ := endian.get_u16(buffer[0:2], .Little)

	for _ in 0..<num_runs {
		io.read_at_least(r, buffer[:], 2)
		start, _ := endian.get_u16(buffer[0:2], .Little)

		io.read_at_least(r, buffer[:], 2)
		length, _ := endian.get_u16(buffer[0:2], .Little)

		run := Run { start = u16be(start), length = u16be(length) }
		append(&rc.run_list, run)
	}

	rb.containers[ci.key] = rc
	cindex_ordered_insert(rb, ci.key)
}

reader_init_from_file :: proc(
	filepath: string,
) -> (r: io.Reader, err: os.Error) {
	fh: os.Handle = os.open(filepath) or_return
	st: io.Stream = os.stream_from_handle(fh)
	r = io.to_reader(st)
	return r, nil
}
