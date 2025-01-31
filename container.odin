package roaring

import "base:intrinsics"
import "base:runtime"
import "core:slice"

@(private)
container_destroy :: proc(container: Container) {
	switch c in container {
	case Array_Container:
		array_container_destroy(c)
	case Bitmap_Container:
		// TODO: Deprecate this.
		// bitmap_container_destroy(c)
	case Run_Container:
		run_container_destroy(c)
	}
}

@(private, require_results)
container_get_cardinality :: proc(container: Container) -> (cardinality: int) {
	switch c in container {
	case Array_Container:
		cardinality = c.cardinality
	case Bitmap_Container:
		cardinality = c.cardinality
	case Run_Container:
		cardinality = run_container_get_cardinality(c)
	}
	return cardinality
}

@(private)
container_set_cardinality :: proc(container: ^Container) {
	switch &c in container {
	case Array_Container:
		c.cardinality = array_container_get_cardinality(c)
	case Bitmap_Container:
		c.cardinality = bitmap_container_get_cardinality(c)
	case Run_Container:
	}
}

@(private, require_results)
container_is_full :: proc(container: Container) -> bool {
	switch c in container {
	// Array_Container can never be full because it can only hold 4096 values,
	// and a container can contain 65536 values at the max.
	case Array_Container:
		return false
	case Bitmap_Container:
		// and_bc := proc(byte1: u8, byte2: u8) -> u8 {
		// 	return byte1 & byte2
		// }

		res := c.bitmap[0]
		for v in c.bitmap {
			res &= v
		}
		// res: u8 = slice.reduce(c.bitmap[:], u8(0b11111111), and_bc)
		return res == 0b11111111
	case Run_Container:
		rl := c.run_list
		return len(rl) == 1 && rl[0] == Run{0, 65535}
	}
	return false
}

// Converts a given container into its optimal representation, using a
// variety of heuristics.
@(private)
container_convert_to_optimal :: proc(
	container: Container,
	allocator := context.allocator
) -> (optimal: Container, err: runtime.Allocator_Error)  {
	switch c in container {
	case Array_Container:
		if len(c.packed_array) <= MAX_ARRAY_LENGTH {
			optimal = c
		}

		bc := array_container_convert_to_bitmap_container(c, allocator) or_return
		if bitmap_container_should_convert_to_run(bc) {
			optimal = bitmap_container_convert_to_run_container(bc, allocator) or_return
		} else {
			optimal = bc
		}
	case Bitmap_Container:
		if c.cardinality <= MAX_ARRAY_LENGTH {
			optimal = bitmap_container_convert_to_array_container(c, allocator) or_return
		}

		if bitmap_container_should_convert_to_run(c) {
			optimal = bitmap_container_convert_to_run_container(c, allocator) or_return
		} else {
			optimal = c
		}
	case Run_Container:
		cardinality := run_container_get_cardinality(c)

		// "If the run container has cardinality greater than 4096 values, then it
		// must contain no more than ⌈(8192 − 2)/4⌉ = 2047 runs."
		// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
		if cardinality > MAX_ARRAY_LENGTH {
			if len(c.run_list) <= MAX_RUNS_PERMITTED {
				optimal = c
			} else {
				optimal = run_container_convert_to_bitmap_container(c)
			}

		// "If the run container has cardinality no more than 4096, then the number
		// of runs must be less than half the cardinality."
		// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
		//
		// If the number of runs is *more* than half the cardinality, we can create
		// a Array_Container, because we know that there are less than 4096 value
		// and thus the packed array will be more efficient than a bitmap or run list.
		} else {
			if len(c.run_list) < (cardinality / 2) {
				optimal = c
			} else {
				optimal = run_container_convert_to_array_container(c, allocator) or_return
			}
		}
	}

	return optimal, nil
}

// Takes any container, and returns a fresh clone of it as a new Bitmap_Container.
// In the case of Array_Container and Run_Container as input, we will perform a
// conversion.
@(private)
container_clone_to_bitmap :: proc(
	container: Container,
	allocator := context.allocator,
) -> (bc: Bitmap_Container, err: runtime.Allocator_Error) {
	cloned := container_clone(container, allocator) or_return

	switch c in cloned {
	case Array_Container:
		bc = array_container_convert_to_bitmap_container(c) or_return
	case Bitmap_Container:
		bc = c
	case Run_Container:
		bc = run_container_convert_to_bitmap_container(c)
	}

	return bc, nil
}

// Clones any Container to a new version of itself.
@(private)
container_clone :: proc(
	container: Container,
	allocator := context.allocator,
) -> (cloned: Container, err: runtime.Allocator_Error) {
	switch c in container {
	case Array_Container:
		new_packed_array := slice.clone_to_dynamic(c.packed_array[:], allocator) or_return
		cloned = Array_Container{
			packed_array=new_packed_array,
			cardinality=c.cardinality,
		}
	case Bitmap_Container:
		cloned = Bitmap_Container{
			bitmap=c.bitmap,
			cardinality=c.cardinality,
		}
	case Run_Container:
		new_run_list := slice.clone_to_dynamic(c.run_list[:], allocator) or_return
		cloned = Run_Container{
			run_list=cast(Run_List)new_run_list,
		}
	}

	return cloned, nil
}

@(private)
container_flip :: proc(
	rb: ^Roaring_Bitmap,
	container_idx: u16,
	start: u16,
	end: u16,
) -> (ok: bool, err: runtime.Allocator_Error) {
	// If the current container is *not* in the Roaring_Bitmap, that means it contains all
	// zeros and we can create a new container set to 1 (a full Run_List).
	if !(container_idx in rb.containers) {
		rc := run_container_init() or_return
		append(&rc.run_list, Run{start, end - start})
		rb.containers[container_idx] = rc
		cindex_ordered_insert(rb, container_idx)
		return
	}

	// If the container is in the Roaring_Bitmap and its values are all 1s, and we
	// want to flip the full range of the container, then delete it.
	container := rb.containers[container_idx]
	if container_is_full(container) && start == 0 && end == 65535 {
		free_at(rb, container_idx)
		return
	}

	// Otherwise flip with a given range.
	switch &c in container {
	// "When applied to an array container, the flip function uses a binary search
	// to first determine all values contained in the range. We can then determine
	// whether the result of the flip should be an array container or a bitmap
	// container. If the output is an array container, then the flip can be done
	// in- place, assuming that there is enough capacity in the container,
	// otherwise a new buffer is allocated. If the output must be a bitmap
	// container, the array container is converted to a bitmap container and
	// flipped."
	// Ref: https://arxiv.org/pdf/1603.06549 (Page 13)
	case Array_Container:
		first_one_i, _ := slice.binary_search(c.packed_array[:], start)

		cursor := start
		offset := 0
		array_cursor := first_one_i
		array_val := c.packed_array[array_cursor]

		outer: for {
			if (first_one_i + offset) >= len(c.packed_array) {
				break outer
			}
			array_val = c.packed_array[first_one_i + offset]

			if cursor < array_val {
				inject_at(&c.packed_array, first_one_i + offset, cursor)
				offset += 1
			} else if cursor == array_val {
				ordered_remove(&c.packed_array, first_one_i + offset)
			} else {
				array_cursor += 1

			}
			cursor += 1
		}

		for v in cursor..=end {
			append(&c.packed_array, v)
			c.cardinality += 1
		}
		c.cardinality = array_container_get_cardinality(c)

	// "Flipping a bitmap container can be done in-place, if needed, using a
	// procedure similar to Algorithm 3."
	// Ref: https://arxiv.org/pdf/1603.06549 (Page 13)
	case Bitmap_Container:
		cursor := start
		start_byte := start / 8
		end_byte := (end / 8) + 1

		// FIXME: Just using the raw bitmap_container_add/bitmap_container_remove procs
		// at the beginning/end here instead of doing any fancy bit flipping..
		//
		// I figure we only have to run this at max 8 times per flip, so it's not a huge
		// perf. hit. An optimization area for the future though when I have time.
		for byte_i: u16 = start_byte; byte_i <= end_byte; byte_i += 1 {
			bm := c.bitmap[byte_i]

			// If at the start, flip from the starting position until we either:
			// - hit the end of the byte
			// - set all the bits we need to
			if byte_i == start_byte {
				start_bit := start - (byte_i * 8)
				left_to_flip := end - cursor
				end_bit := min(start_bit + left_to_flip, 7)
				for _ in start_bit..=end_bit {
					if bitmap_container_contains(c, cursor) {
						bitmap_container_remove(&c, cursor)
					} else {
						bitmap_container_add(&c, cursor)
					}
					cursor += 1
				}

			// If at the end, flip from 0 until we have finished setting
			// all the required bits.
			} else if byte_i == (end_byte - 1) {
				bits_to_flip := end - cursor
				for _ in 0..=bits_to_flip {
					if bitmap_container_contains(c, cursor) {
						bitmap_container_remove(&c, cursor)
					} else {
						bitmap_container_add(&c, cursor)
					}
					cursor += 1
				}

			// Otherwise we are in an entire byte that needs to be flipped.
			// Flip it all and increase our cursor by 8.
			} else {
				c.cardinality -= int(intrinsics.count_ones(bm))
				bm = intrinsics.reverse_bits(bm)
				c.cardinality += int(intrinsics.count_ones(bm))
				cursor += 8
			}
		}

	// "In flipping a run container, we always first compute the result as a run
	// container. When the container’s capacity permits, an in-place flip avoids
	// memory allocation. This should be a common situation, because flipping
	// increases the number of runs by at most one. Thus, there is a capacity
	// problem only when the number of runs increases and the original runs fit
	// exactly within the array."
	//
	// FIXME: Currently doing this the cheap way by converting to a bitmap, then flipping,
	// and then converting back to either a Run_Container or Bitmap_Container.
	case Run_Container:
		rb.containers[container_idx] = run_container_convert_to_bitmap_container(c)
		container_flip(rb, container_idx, start, end)
		container = container_convert_to_optimal(rb.containers[container_idx]) or_return
	}

	
	// Remove this container entirely if we no longer have any elements in it.
	if container_get_cardinality(container) == 0 {
		free_at(rb, container_idx)
	} else {
		rb.containers[container_idx] = container
	}

	return true, nil
}
