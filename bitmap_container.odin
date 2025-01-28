package roaring

import "base:builtin"
import "base:intrinsics"
import "base:runtime"

@(private, require_results)
bitmap_container_init :: proc(
	allocator := context.allocator
) -> (Bitmap_Container, runtime.Allocator_Error) {
	arr, err := new([8192]u8, allocator)
	bc := Bitmap_Container{
		bitmap=arr,
		cardinality=0,
	}
	return bc, err
}

bitmap_container_destroy :: proc(bc: Bitmap_Container) {
	free(bc.bitmap)
}

@(private)
bitmap_container_add :: proc(
	bc: ^Bitmap_Container,
	n: u16be,
) -> (ok: bool, err: runtime.Allocator_Error) {
	bitmap := bc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)
	mask := u8(1 << bit_i)
	byte := bitmap[byte_i]
	bitmap[byte_i] = byte | mask

	bc.bitmap = bitmap
	bc.cardinality += 1

	return true, nil
}

@(private)
bitmap_container_remove :: proc(
	bc: ^Bitmap_Container,
	n: u16be,
) -> (ok: bool, err: runtime.Allocator_Error) {
	bitmap := bc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)
	mask := u8(1 << bit_i)

	byte := bitmap[byte_i]
	bitmap[byte_i] = byte & ~mask

	bc.bitmap = bitmap
	bc.cardinality -= 1

	return true, nil
}

@(private)
bitmap_container_contains :: proc(bc: Bitmap_Container, n: u16be) -> (found: bool) {
	bitmap := bc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)

	// How to check if a specific bit is set:
	// 1. Store as 'temp': left shift 1 by k to create a number that has
	//    only the k-th bit set.
	// 2. If bitwise AND of n and 'temp' is non-zero, then the bit is set.
	byte := bitmap[byte_i]
	found = (byte & (1 << bit_i)) != 0

	return found
}

// Sets a range of bits from 0 to 1 in a Bitmap_Container bitmap.
// Ref: https://arxiv.org/pdf/1603.06549 (Page 11)
//
// TODO: Update to u16be params instead of int
@(private)
bitmap_container_set_range :: proc(bc: ^Bitmap_Container, start: int, length: int) {
	end := start + length

	x1 := start / 8
	y1 := (end - 1) / 8
	z := 0b11111111

	x2 := z << u8(start % 8)
	y2 := z >> u8(8 - (end % 8) % 8)

	if x1 == y1 {
		bc.bitmap[x1] = bc.bitmap[x1] | u8(x2 & y2)
	} else {
		bc.bitmap[x1] = bc.bitmap[x1] | u8(x2)
		for k := x1 + 1; k < y1; k += 1 {
			bc.bitmap[k] = bc.bitmap[k] | u8(z)
		}
		bc.bitmap[y1] = bc.bitmap[y1] | u8(y2)
	}
}

// Sets a range of bits from 1 to 0 in a Bitmap_Container bitmap.
// Ref: https://arxiv.org/pdf/1603.06549 (Page 11)
@(private)
bitmap_container_unset_range :: proc(bc: ^Bitmap_Container, start: int, length: int) {
	end := start + length

	x1 := start / 8
	y1 := (end - 1) / 8
	z := 0b11111111

	x2 := z << u8(start % 8)
	y2 := z >> u8(8 - (end % 8) % 8)

	if x1 == y1 {
		bc.bitmap[x1] = bc.bitmap[x1] &~ u8(x2 & y2)
	} else {
		bc.bitmap[x1] = bc.bitmap[x1] &~ u8(x2)
		for k := x1 + 1; k < y1; k += 1 {
			bc.bitmap[k] = bc.bitmap[k] &~ u8(z)
		}
		bc.bitmap[y1] = bc.bitmap[y1] &~ u8(y2)
	}
}

// Counts the no. of runs in a bitmap (eg., Bitmap_Container).
// Ref: https://arxiv.org/pdf/1603.06549 (Page 7, Algorithm 1)
@(private)
bitmap_container_count_runs :: proc(bc: Bitmap_Container) -> (count: int) {
	for i in 0..<(BYTES_PER_BITMAP - 1) {
		byte := bc.bitmap[i]
		count += intrinsics.count_ones(int(byte << 1) &~ int(byte)) + int((byte >> 7) &~ bc.bitmap[i + 1])
	}

	byte := bc.bitmap[BYTES_PER_BITMAP - 1]
	count += intrinsics.count_ones(int(byte << 1) &~ int(byte)) + int((byte >> 7))

	return count
}

// Checks if a Bitmap_Container should be converted to a Run_Container. This is true
// when the number of runs in a Bitmap_Container is >= 2048. Because it can be
// expensive to count all the runs, we break out after this lower bound is met.
//
// "... the computation may be expensive—exceeding the cost of computing
// the union or intersection between two bitmap containers. Thus, instead of
// always computing the number of runs exactly, we rely on the observation that no
// bitmap container with more than 2047 runs should be converted. As soon as we
// can produce a lower bound exceeding 2047 on the number of runs, we can stop. An
// exact computation of the number of runs is important only when our lower bound
// is less than 2048."
//
// Ref: https://arxiv.org/pdf/1603.06549 (Page 7)
@(private)
bitmap_container_should_convert_to_run :: proc(bc: Bitmap_Container) -> bool {
	run_count := bitmap_container_count_runs(bc)

	// "If the run container has cardinality no more than 4096, then the number of
	// runs must be less than half the cardinality."
	// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
	return run_count < (bc.cardinality / 2)
}

// Finds the cardinality of a Bitmap_Container by finding all the set bits.
@(private)
bitmap_container_get_cardinality :: proc(bc: Bitmap_Container) -> (acc: int) {
	for byte in bc.bitmap {
		if byte != 0 {
			acc += intrinsics.count_ones(int(byte))
		}
	}

	return acc
}

// Bitmap vs Bitmap: To compute the intersection between two bitmaps, we first
// compute the cardinality of the result using the bitCount function over the
// bitwise AND of the corresponding pairs of words. If the intersection exceeds
// 4096, we materialize a bitmap container by recomputing the bitwise AND between
// the words and storing them in a new bitmap container. Otherwise, we generate a
// new array container by, once again, recomputing the bitwise ANDs, and iterating
// over their 1-bits.
@(private)
bitmap_container_and_bitmap_container :: proc(
	bc1: Bitmap_Container,
	bc2: Bitmap_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	count := 0
	for byte1, i in bc1.bitmap {
		byte2 := bc2.bitmap[i]
		res := byte1 & byte2
		count += intrinsics.count_ones(int(res))
	}

	if count > MAX_ARRAY_LENGTH {
		bc := bitmap_container_init(allocator) or_return
		for byte1, i in bc1.bitmap {
			byte2 := bc2.bitmap[i]
			res := byte1 & byte2
			bc.bitmap[i] = res
			bc.cardinality += intrinsics.count_ones(int(res))
		}
		c = bc
	} else {
		ac := array_container_init(allocator) or_return
		for byte1, i in bc1.bitmap {
			byte2 := bc2.bitmap[i]
			res := byte1 & byte2
			for j in 0..<8 {
				bit_is_set := (res & (1 << u8(j))) != 0
				if bit_is_set {
					total_i := u16be((i * 8) + j)
					array_container_add(&ac, total_i) or_return
				}
			}
		}
		c = ac
	}

	return c, nil
}

// "The intersection between a run container and a bitmap container begins by
// checking the cardinality of the run container. If it is no larger than 4096,
// then we create an initially empty array container. We then iterate over all
// integers contained in the run container, and check, one by one, whether they
// are contained in the bitmap container: when an integer is found to be in the
// intersection, it is appended to the output in the array container. The running
// time of this operation is determined by the cardinality of the run container.
// Otherwise, if the input run container is larger than 4096, then we create a
// copy of the input bitmap container. Using fast bitwise operations, we set to
// zero all bits corresponding to the complement of the run container (see
// Algorithm 3). We then check the cardinality of the result, converting to an
// array container if needed."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 10, 11)
@(private)
bitmap_container_and_run_container :: proc(
	bc: Bitmap_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	if container_get_cardinality(rc) <= MAX_ARRAY_LENGTH {
		nc_ac := array_container_init(allocator) or_return
		for run in rc.run_list {
			for i := run.start; i <= run_end_position(run); i += 1 {
				if bitmap_container_contains(bc, u16be(i)) {
					array_container_add(&nc_ac, u16be(i)) or_return
				}
			}
		}
		return nc_ac, nil
	} else {
		c = container_clone(bc, allocator) or_return
		new_bc := c.(Bitmap_Container)

		// Set the complement of the Run_List to be zero.
		for run, i in rc.run_list {
			if i == 0 && run.start > 0 {
				bitmap_container_unset_range(&new_bc, 0, int(run.length + 1))
			} else if i > 0 {
				prev_run := rc.run_list[i - 1]
				complement_start := run.start - prev_run.start + 1
				complement_length := run.start - complement_start
				bitmap_container_unset_range(&new_bc, int(complement_start), int(complement_length))
			}
		}

		// Set any remaining bits after the last Run to be 0.
		last_run := rc.run_list[len(rc.run_list) - 1]
		unset_start := int(run_after_end_pos(last_run))
		unset_length := (BYTES_PER_BITMAP * 8) - unset_start
		bitmap_container_unset_range(&new_bc, unset_start, unset_length)

		// Determine the cardinality.
		acc := 0
		for byte in new_bc.bitmap {
			acc += intrinsics.count_ones(int(byte))
		}
		new_bc.cardinality = acc

		// Convert down to a Array_Container if needed.
		if new_bc.cardinality <= MAX_ARRAY_LENGTH {
			return bitmap_container_convert_to_array_container(new_bc, allocator)
		} else {
			return new_bc, nil
		}
	}
}

// Performs an ANDNOT operation between two Bitmap_Container and returns the result
// as a new Bitmap_Container.
@(private)
bitmap_container_andnot_bitmap_container :: proc(
	bc1: Bitmap_Container,
	bc2: Bitmap_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	set_count := 0
	bc := bitmap_container_init(allocator) or_return

	for byte1, i in bc1.bitmap {
		byte2 := bc2.bitmap[i]
		res := byte1 &~ byte2
		set_count += intrinsics.count_ones(int(res))
		bc.bitmap[i] = res
		bc.cardinality += intrinsics.count_ones(int(res))
	}

	// Convert down to an array container if that makes sense here.
	if set_count <= MAX_ARRAY_LENGTH {
		c = bitmap_container_convert_to_array_container(bc) or_return
	} else {
		c = bc
	}

	return c, nil
}

// Performs an XOR operation between two Bitmap_Container and returns the result
// as a new Bitmap_Container.
@(private)
bitmap_container_xor_bitmap_container :: proc(
	bc1: Bitmap_Container,
	bc2: Bitmap_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	set_count := 0
	bc := bitmap_container_init(allocator) or_return

	for byte1, i in bc1.bitmap {
		byte2 := bc2.bitmap[i]
		res := byte1 ~ byte2
		set_count += intrinsics.count_ones(int(res))
		bc.bitmap[i] = res
		bc.cardinality += intrinsics.count_ones(int(res))
	}

	// Convert down to an array container if that makes sense here.
	if set_count <= MAX_ARRAY_LENGTH {
		c = bitmap_container_convert_to_array_container(bc) or_return
	} else {
		c = bc
	}

	return c, nil
}

// A union between two bitmap containers is straightforward: we execute the
// bitwise OR between all pairs of corresponding words. There are 1024 words in
// each container, so 1024 bitwise OR operations are needed. At the same time, we
// compute the cardinality of the result using the bitCount function on the
// generated words.
@(private)
bitmap_container_or_bitmap_container :: proc(
	bc1: Bitmap_Container,
	bc2: Bitmap_Container,
	allocator := context.allocator,
) -> (new_bc: Bitmap_Container, err: runtime.Allocator_Error) {
	new_bc = bitmap_container_init(allocator) or_return
	for byte, i in bc1.bitmap {
		res := byte | bc2.bitmap[i]
		new_bc.bitmap[i] = res
		new_bc.cardinality = intrinsics.count_ones(int(res))
	}
	return new_bc, nil
}

// "The union between a run container and a bitmap container is computed by first
// cloning the bitmap container. We then set to one all bits corresponding to the
// integers in the run container, using fast bitwise OR operations (see again
// Algorithm 3)."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 11)
@(private)
bitmap_container_or_run_container :: proc(
	bc: Bitmap_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> (new_bc: Bitmap_Container, err: runtime.Allocator_Error) {
	c := container_clone(bc, allocator) or_return
	new_bc = c.(Bitmap_Container)

	for run in rc.run_list {
		bitmap_container_set_range(&new_bc, int(run.start), int(run.length + 1))
	}

	new_bc.cardinality = bitmap_container_get_cardinality(new_bc)
	return new_bc, nil
}

// Bitmap_Container => Array_Container
@(private)
bitmap_container_convert_to_array_container :: proc(
	bc: Bitmap_Container,
	allocator := context.allocator,
) -> (ac: Array_Container, err: runtime.Allocator_Error) {
	ac = array_container_init(allocator) or_return

	for byte, i in bc.bitmap {
		for j in 0..<8 {
			bit_is_set := (byte & (1 << u8(j))) != 0
			if bit_is_set {
				total_i := u16be((i * 8) + j)
				array_container_add(&ac, total_i) or_return
			}
		}
	}

	bitmap_container_destroy(bc)
	return ac, nil
}

// Bitmap_Container => Run_Container
// Ref: https://arxiv.org/pdf/1603.06549 (Page 8)
@(private)
bitmap_container_convert_to_run_container :: proc(
	bc: Bitmap_Container,
	allocator := context.allocator
) -> (rc: Run_Container, err: runtime.Allocator_Error) {
	rc = run_container_init(allocator) or_return

	i := 1
	byte := bc.bitmap[i-1]
	for i <= BYTES_PER_BITMAP {
		if byte == 0b00000000 {
			i += 1

			if i > BYTES_PER_BITMAP {
				break
			}

			byte = bc.bitmap[i-1]
			continue
		}

		j := intrinsics.count_trailing_zeros(byte)
		x := int(j) + 8 * (i - 1)
		byte = byte | (byte - 1)

		for i + 1 <= BYTES_PER_BITMAP && byte == 0b11111111 {
			i += 1
			byte = bc.bitmap[i-1]
		}

		y: int
		if byte == 0b11111111 {
			y = 8 * i
		} else {
			// Finds the index of the least significant zero bit.
			// Ex. '00011011' => 2
			k := intrinsics.count_trailing_zeros(int(~byte))
			y = k + 8 * (i - 1)
		}

		run := Run{start=u16be(x), length=u16be(y - x - 1)}
		append(&rc.run_list, run)

		byte = byte & (byte + 1)
	}

	bitmap_container_destroy(bc)
	return rc, nil
}
