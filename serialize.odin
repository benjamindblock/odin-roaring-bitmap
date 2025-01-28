package roaring

import "core:os"

@(private)
handle_from_file :: proc(filepath: string) -> (fh: os.Handle, err: os.Error) {
	flags: int = os.O_WRONLY | os.O_CREATE | os.O_TRUNC

	mode: int = 0
	when os.OS == .Linux || os.OS == .Darwin {
		// NOTE(justasd): 644 (owner read, write; group read; others read)
		mode = os.S_IRUSR | os.S_IWUSR | os.S_IRGRP | os.S_IROTH
	}

	fh = os.open(filepath, flags, mode) or_return
	return fh, nil
}

serialize :: proc(filepath: string, rb: Roaring_Bitmap) -> Roaring_Error {
	fh := handle_from_file(filepath) or_return
	defer os.close(fh)

	write_header(fh, rb)
	write_containers(fh, rb)

	return nil
}

write_header :: proc(fh: os.Handle, rb: Roaring_Bitmap) -> (n: int) {
	n += write_cookie_header(fh, rb)
	n += write_descriptive_header(fh, rb)
	n += write_offset_header(fh, rb, n)
	return n
}

write_cookie_header :: proc(fh: os.Handle, rb: Roaring_Bitmap) -> (n: int) {
	has_run_containers := is_optimized(rb)

	if has_run_containers {
		n += write_u16le(fh, SERIAL_COOKIE)
		n += write_u16le(fh, len(rb.cindex) - 1)

		// Write out the bitmap indicating whether each container is a
		// Run_Container or not.
		i: u8 = 0
		byte: u8 = 0
		for key in rb.cindex {
			container := rb.containers[key]
			if i == 8 {
				n += write_u8(fh, byte)
				i = 0
				byte = 0
			}

			_, is_run_container := container.(Run_Container)
			if is_run_container {
				byte = (1 << i) | byte
			}

			i += 1
		}

		if i < 8 {
			n += write_u8(fh, byte)
		}
	} else {
		n += write_u16le(fh, SERIAL_COOKIE_NO_RUNCONTAINER)
		n += write_u16le(fh, 0)
		n += write_u32le(fh, len(rb.cindex))
	}

	return n
}

write_descriptive_header :: proc(fh: os.Handle, rb: Roaring_Bitmap) -> (n: int) {
	for key in rb.cindex {
		container := rb.containers[key]
		cardinality := container_get_cardinality(container) - 1

		n += write_u16le(fh, int(key))
		n += write_u16le(fh, cardinality)
	}

	return n
}

// If and only if one of these is true
// - the cookie takes value SERIAL_COOKIE_NO_RUNCONTAINER
// - the cookie takes the value SERIAL_COOKIE and there are at least NO_OFFSET_THRESHOLD containers,
// then we store (using a 32-bit value) the location (in bytes) of the container
// from the beginning of the stream (starting with the cookie) for each container.
write_offset_header :: proc(fh: os.Handle, rb: Roaring_Bitmap, offset: int) -> (n: int) {
	offset := offset
	has_run_containers := is_optimized(rb)

	if (has_run_containers && len(rb.cindex) >= NO_OFFSET_THRESHOLD) || !has_run_containers {
		for key in rb.cindex {
			n += write_u32le(fh, offset)
			container := rb.containers[key]
			size := sizeof_container_bytes(container)
			offset += size
		}
	}

	return n
}

sizeof_container_bytes :: proc(container: Container) -> int {
	switch c in container {
	case Array_Container:
		return 2 * len(c.packed_array)
	case Bitmap_Container:
		return BYTES_PER_BITMAP
	case Run_Container:
		return 2 + (4 * len(c.run_list))
	}

	return 0
}

write_containers :: proc(fh: os.Handle, rb: Roaring_Bitmap) -> (n: int) {
	for key in rb.cindex {
		switch c in rb.containers[key] {
		case Array_Container:
			for v in c.packed_array {
				n += write_u16le(fh, int(v))
			}
		case Bitmap_Container:
			for b in c.bitmap {
				n += write_u8(fh, b)
			}
		case Run_Container:
			n += write_u16le(fh, len(c.run_list))
			for run in c.run_list {
				n += write_u16le(fh, int(run.start))
				n += write_u16le(fh, int(run.length))
			}
		}
	}

	return n
}

write_u8 :: proc(fh: os.Handle, v: u8) -> (n: int) {
	as_bytes := transmute([1]byte)v
	os.write(fh, as_bytes[:])
	return 1
}

write_u16le :: proc(fh: os.Handle, v: int) -> (n: int) {
	as_bytes := transmute([2]byte)u16le(v)
	os.write(fh, as_bytes[:])
	return 2
}

write_u32le :: proc(fh: os.Handle, v: int) -> (n: int) {
	as_bytes := transmute([4]byte)u32le(v)
	os.write(fh, as_bytes[:])
	return 4
}
