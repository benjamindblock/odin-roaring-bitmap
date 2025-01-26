package roaring

import "core:fmt"
import "core:io"
import "core:os"
import "core:encoding/endian"

SERIAL_COOKIE_NO_RUNCONTAINER :: 12346
SERIAL_COOKIE :: 12347
NO_OFFSET_THRESHOLD :: 4

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

	return ri, true
}

reader_init_from_file :: proc(
	filepath: string,
) -> (r: io.Reader, err: os.Error) {
	fh: os.Handle = os.open(filepath) or_return
	st: io.Stream = os.stream_from_handle(fh)
	r = io.to_reader(st)
	return r, nil
}
