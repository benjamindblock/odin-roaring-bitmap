package roaring

Roaring_Bitmap_Iterator :: struct {
	rb: ^Roaring_Bitmap,
	cardinality: int,
	overall_idx: int,    // Progress amongst all set values
	container_idx: int,  // The container in the cindex we are in
	word_idx: int,       // The sub-container position (eg., byte, Run) we are at
	bit_idx: u16be,      // The position in the sub-container
}

make_iterator :: proc(rb: ^Roaring_Bitmap) -> Roaring_Bitmap_Iterator {
	it := Roaring_Bitmap_Iterator {
		rb = rb,
		cardinality = get_cardinality(rb^),
	}

	return it
}

// the Roaring_Bitmap in order...
// Returns the next bit, including its set-state. ok=false once exhausted
// Inputs:
//   - it: The iterator that holds the state.
// Returns:
//   - set: `true` if the bit at `index` is set.
//   - index: The next bit of the Bit_Array referenced by `it`.
//   - ok: `true` if the iterator can continue, `false` if the iterator is done
iterate_set_values :: proc (it: ^Roaring_Bitmap_Iterator) -> (v: u32, index: int, ok: bool) {
	main_loop: for {
		if it.overall_idx >= it.cardinality {
			return 0, 0, false
		}

		// Get the current container
		key := it.rb.cindex[it.container_idx]
		container := it.rb.containers[key]

		switch c in container {
		// In an Array_Container:
		//   - word_idx: always set to 0 (there is just one array with all the values)
		//   - bit_idx: the current array position in the array
		case Array_Container:
			// Top 16 bits of the number that is set.
			most_significant := key

			// Bottom 16 bits of the number that is set.
			least_significant := c.packed_array[it.bit_idx]

			// Recreate the original value.
			v = u32(transmute(u32be)[2]u16be{most_significant, least_significant})

			it.bit_idx += 1

			// If we have reached the end of the array (and the container), then
			// advance to the next container.
			if int(it.bit_idx) >= c.cardinality {
				it.bit_idx = 0
				it.word_idx = 0
				it.container_idx += 1
			}
			break main_loop

		// In Bitmap_Container:
		//   - word_idx: index of the current byte in the bitmap
		//   - bit_idx: the current bit in the bitmap
		case Bitmap_Container:
			outer: for {
				byte := c.bitmap[it.word_idx]

				// Scan for the next set bit in this word.
				for (byte & (1 << it.bit_idx)) == 0 {
					// If we reached the end of the bitmap, move to the next container.
					if it.bit_idx >= 8 && it.word_idx >= BYTES_PER_BITMAP - 1 {
						it.bit_idx = 0
						it.word_idx = 0
						it.container_idx += 1
						continue main_loop
					// If we reached the end of the word, move to the next one in this
					// bitmap search loop.
					} else if it.bit_idx >= 8 {
						it.bit_idx = 0
						it.word_idx += 1
						continue outer
					// Otherwise look at the next bit.
					} else {
						it.bit_idx += 1
					}
				}

				most_significant := key
				least_significant := (it.word_idx * 8) + int(it.bit_idx)
				v = u32(transmute(u32be)[2]u16be{most_significant, u16be(least_significant)})

				it.bit_idx += 1
				break main_loop
			}

		// In Run_Container:
		//   - word_idx: tracks the current Run in the Run_List
		//   - bit_idx: the current position in the Run
		case Run_Container:
			run := c.run_list[it.word_idx]

			most_significant := key
			least_significant := run.start + it.bit_idx

			// Recreate the original value.
			v = u32(transmute(u32be)[2]u16be{most_significant, u16be(least_significant)})

			it.bit_idx += 1

			// If we have reached the end of the Run, advance to the next one
			// in the Run_List.
			if it.bit_idx >= run.length + 1 {
				it.bit_idx = 0
				it.word_idx += 1
			}

			// If we have moved beyond the Run_List, move to the next container.
			if it.word_idx >= len(c.run_list) {
				it.container_idx += 1
			}

			break main_loop
		}
	}


	index = it.overall_idx
	it.overall_idx += 1
	return v, index, true
}
