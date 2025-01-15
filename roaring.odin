package roaring

import "base:builtin"
import "base:intrinsics"
import "core:fmt"
import "core:mem"
import "core:slice"

MAX_RUNS_PERMITTED :: 2048

Run :: struct {
	start: int,
	length: int,
}

Run_List :: distinct [dynamic]Run

Roaring_Error :: union {
	Already_Set_Error,
	Not_Set_Error,
}

Already_Set_Error :: struct {
	value: u16be,
}

Not_Set_Error :: struct {
	value: u16be,
}

// "An array container is an object containing a counter keeping track of the
// number of integers, followed by a packed array of sorted 16-bit unsigned
// integers. It can be serialized as an array of 16-bit values."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 5)
Array_Container :: struct {
	packed_array: [dynamic]u16be,
	cardinality: int,
}

// NOTE: I use a 8192 8-bit words instead of 1024 64-bit words.
// "A bitmap container is an object made of 1024 64-bit words (using 8 kB)
// representing an uncompressed bitmap, able to store all sets of 16-bit integers.
// The container can be serialized as an array of 64-bit words. We also maintain a
// counter to record how many bits are set to 1."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 5)
Bitmap_Container :: struct {
	bitmap: [dynamic]u8,
	cardinality: int,
}

// "Unlike an array or bitmap container, a run container does not keep track of its
// cardinality; its cardinality can be computed on the fly by summing the lengths
// of the runs. In most applications, we expect the number of runs to be often
// small: the computation of the cardinality should not be a bottleneck."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
Run_Container :: struct {
	run_list: Run_List,
}

Container :: union {
	Array_Container,
	Bitmap_Container,
	Run_Container,
}

Container_Index :: distinct map[u16be]Container

Roaring_Bitmap :: struct {
	index: Container_Index,
	allocator: mem.Allocator,
}

roaring_init :: proc(allocator := context.allocator) -> Roaring_Bitmap {
	index := make(Container_Index)
	return Roaring_Bitmap{index=index, allocator=allocator}
}

roaring_free :: proc(rb: ^Roaring_Bitmap) {
	for i, _ in rb.index {
		roaring_free_at(rb, i)
	}
	delete(rb.index)
}

roaring_free_at :: proc(rb: ^Roaring_Bitmap, i: u16be) {
	container := rb.index[i]
	switch c in container {
	case Array_Container:
		array_container_free(c)
	case Bitmap_Container:
		bitmap_container_free(c)
	case Run_Container:
		run_container_free(c)
	}
	delete_key(&rb.index, i)
}

// If a container doesn’t already exist then create a new array container,
// add it to the Roaring bitmap’s first-level index, and add N to the array.
roaring_set :: proc(
	rb: ^Roaring_Bitmap,
	n: int,
) -> (ok: bool, err: Roaring_Error) {
	nbe := u32be(n)
	i := most_significant(nbe)
	j := least_significant(nbe)

	if !(i in rb.index) {
		rb.index[i] = array_container_init(rb.allocator)
		return roaring_set(rb, n)
	}

	container := &rb.index[i]
	switch &c in container {
	case Array_Container:
		// If an array container has 4,096 integers, first convert it to a
		// Bitmap_Container and then set the bit.
		if c.cardinality == 4096 {
			rb.index[i] = convert_container_array_to_bitmap(c, rb.allocator)
			return roaring_set(rb, n)
		} else {
			set_array_container(&c, j) or_return
		}
	case Bitmap_Container:
		set_bitmap_container(&c, j) or_return
	case Run_Container:
		set_run_container(&c, j) or_return
	}

	return true, nil
}

roaring_unset :: proc(
	rb: ^Roaring_Bitmap,
	n: int,
) -> (ok: bool, err: Roaring_Error) {
	n := u32be(n)
	i := most_significant(n)
	j := least_significant(n)

	if !(i in rb.index) {
		return false, Not_Set_Error{j}
	}

	container := &rb.index[i]
	switch &c in container {
	case Array_Container:
		unset_packed_array(&c, j) or_return
	case Bitmap_Container:
		unset_bitmap(&c, j) or_return
		if c.cardinality <= 4096 {
			rb.index[i] = convert_container_bitmap_to_array(c, rb.allocator)
		}
	case Run_Container:
		unset_run_container(&c, j) or_return
		if len(c.run_list) >= MAX_RUNS_PERMITTED {
			rb.index[i] = convert_container_run_to_bitmap(c, rb.allocator)
		}
	}

	// If we have removed the last element(s) in a container, remove the
	// container + key.
	container = &rb.index[i]
	if container_cardinality(container^) == 0 {
		roaring_free_at(rb, i)
	}

	return true, nil
}

// To check if an integer N exists, get N’s 16 most significant bits (N / 2^16)
// and use it to find N’s corresponding container in the Roaring bitmap.
// If the container doesn’t exist, then N is not in the Roaring bitmap.
// Checking for existence in array and bitmap containers works differently:
//   Bitmap: check if the bit at N % 2^16 is set.
//   Array: use binary search to find N % 2^16 in the sorted array.
roaring_is_set :: proc(rb: Roaring_Bitmap, n: int) -> (found: bool) {
	n := u32be(n)
	i := most_significant(n)
	j := least_significant(n)

	if !(i in rb.index) {
		return false
	}

	container := rb.index[i]
	switch c in container {
	case Array_Container:
		found = is_set_packed_array(c, j)
	case Bitmap_Container:
		found = is_set_bitmap(c, j)
	case Run_Container:
		found = is_set_run_container(c, j)
	}

	return found
}

array_container_init :: proc(allocator := context.allocator) -> Array_Container {
	arr := make([dynamic]u16be, allocator)
	ac := Array_Container{
		packed_array=arr,
		cardinality=0,
	}
	return ac
}

array_container_free :: proc(ac: Array_Container) {
	delete(ac.packed_array)
}

bitmap_container_init :: proc(allocator := context.allocator) -> Bitmap_Container {
	arr := make([dynamic]u8, 8192, allocator)
	bc := Bitmap_Container{
		bitmap=arr,
		cardinality=0,
	}
	return bc
}

bitmap_container_free :: proc(bc: Bitmap_Container) {
	delete(bc.bitmap)
}

run_container_init :: proc(allocator := context.allocator) -> Run_Container {
	run_list := make(Run_List, allocator)
	rc := Run_Container{run_list}
	return rc
}

run_container_free :: proc(rc: Run_Container) {
	delete(rc.run_list)
}

container_cardinality :: proc(container: Container) -> (cardinality: int) {
	switch c in container {
	case Array_Container:
		cardinality = c.cardinality
	case Bitmap_Container:
		cardinality = c.cardinality
	case Run_Container:
		cardinality = run_container_calculate_cardinality(c)
	}
	return cardinality
}

// Returns a u16 in big-endian made up of the 16 most significant
// bits in a u32be number.
@(private)
most_significant :: proc(n: u32be) -> u16be {
	as_bytes := transmute([4]byte)n
	return slice.to_type(as_bytes[0:2], u16be)
}

// Returns a u16 in big-endian made up of the 16 least significant
// bits in a u32be number.
@(private)
least_significant :: proc(n: u32be) -> u16be {
	as_bytes := transmute([4]byte)n
	return slice.to_type(as_bytes[2:4], u16be)
}

set_array_container :: proc(
	ac: ^Array_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	i, found := slice.binary_search(ac.packed_array[:], n)

	if found {
		return false, Already_Set_Error{n}
	}

	inject_at(&ac.packed_array, i, n)
	ac.cardinality += 1

	return true, nil
}

unset_packed_array :: proc(
	ac: ^Array_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	i, found := slice.binary_search(ac.packed_array[:], n)

	if !found {
		return false, Not_Set_Error{n}
	}

	ordered_remove(&ac.packed_array, i)
	ac.cardinality -= 1

	return true, nil
}

is_set_packed_array :: proc(ac: Array_Container, n: u16be) -> (found: bool) {
	_, found = slice.binary_search(ac.packed_array[:], n)		
	return found
}

set_bitmap_container :: proc(
	bc: ^Bitmap_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	if is_set_bitmap(bc^, n) {
		return false, Already_Set_Error{n}
	}

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

unset_bitmap :: proc(
	bc: ^Bitmap_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	if !is_set_bitmap(bc^, n) {
		return false, Not_Set_Error{n}
	}

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

is_set_bitmap :: proc(bc: Bitmap_Container, n: u16be) -> (found: bool) {
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

// Sets a value in a Run_List.
//
// TODO: Cleanup and unify with find_possible_run_by_value, which is used in
// the unset_run_container and is_set_run_container methods.
set_run_container :: proc(
	rc: ^Run_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	if is_set_run_container(rc^, n) {
		return false, Already_Set_Error{n}
	}

	n := int(n)

	if len(rc.run_list) == 0 {
		new_run := Run{start=n, length=1}
		append(&rc.run_list, new_run)
		return true, nil
	}

	cmp := proc(r: Run, n: int) -> (res: slice.Ordering) {
		if n >= (r.start - 1) && n <= run_end(r) {
			res = .Equal
		} else if n < r.start {
			res = .Greater
		} else if n > run_end(r) {
			res = .Less
		} else {
			res = .Equal
		}

		return res
	}

	i, found := slice.binary_search_by(rc.run_list[:], n, cmp)

	if found {
		run_to_expand := &rc.run_list[i]

		// Expand the matching Run backwards.
		if run_to_expand.start - 1 == n {
			run_to_expand.start -= 1
			run_to_expand.length += 1

			// Merge with the previous run if we need to.
			if i - 1 >= 0 {
				prev_run := rc.run_list[i-1]
				if run_to_expand.start == run_end(prev_run) {
					run_to_expand.length += prev_run.length
					run_to_expand.start = prev_run.start
					ordered_remove(&rc.run_list, i-1)
				}
			}

		// Expand a Run forwards.
		} else if run_end(run_to_expand^) == n {
			run_to_expand.length += 1

			// Merge with the next run if we need to.
			if i + 1 < len(rc.run_list) {
				next_run := rc.run_list[i+1]
				if run_end(run_to_expand^) == next_run.start {
					run_to_expand.length += next_run.length
					ordered_remove(&rc.run_list, i+1)
				}
			}
		}
	} else {
		new_run := Run{start=n, length=1}
		inject_at(&rc.run_list, i, new_run)
	}

	return true, nil
}

// Finds the Run that might contain a given value and returns a pointer to it
// for modification.
find_possible_run_by_value :: proc(rl: Run_List, n: int) -> (run: ^Run, index: int, exact_match: bool) {
	if len(rl) == 0 {
		return run, -1, false
	}

	cmp := proc(r: Run, n: int) -> slice.Ordering {
		if r.start < n {
			return .Less
		} else if r.start > n {
			return .Greater
		} else {
			return .Equal
		}
	}

	i, found := slice.binary_search_by(rl[:], n, cmp)

	if found {
		return &rl[i], i, true
	}

	// Because the binary_search_by returns the insertion order if an element
	// is not found, we want to subtract 1 to get the container that the run
	// might actually be in.
	if i > 0 {
		i -= 1
	}

	return &rl[i], i, false
}

// Cases:
// 1. Standalone Run -- remove
// 2. Value at beginning of run -- increment start by 1 and decrease length by 1
// 3. Value at end of run -- decrease length by 1
// 4. Value in middle of run -- split Run into two Runs
unset_run_container :: proc(
	rc: ^Run_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	if !is_set_run_container(rc^, n) {
		return false, Not_Set_Error{n}
	}

	n := int(n)
	run_to_check, index, exact_match := find_possible_run_by_value(rc.run_list, n)

	// 1. Standalone Run -- remove
	if exact_match && run_to_check.length == 1 {
		ordered_remove(&rc.run_list, index)	

	// 2. Value at beginning of run -- increment start by 1 and decrease length by 1
	} else if exact_match {
		run_to_check.start += 1
		run_to_check.length -= 1

	// 3. Value at end of run -- decrease length by 1
	} else if run_end(run_to_check^) - 1 == n {
		run_to_check.length -= 1

	// 4. Value in middle of run -- split Run into two Runs
	} else {
		new_rc := Run{
			start = run_to_check.start,
			length = (n - run_to_check.start),
		}

		run_to_check.start = n + 1
		run_to_check.length = run_to_check.length - (run_to_check.start - new_rc.start)

		inject_at(&rc.run_list, index, new_rc)
	}

	return true, nil
}

// Checks to see if a value is set in a Run_Container.
is_set_run_container :: proc(rc: Run_Container, n: u16be) -> bool {
	if len(rc.run_list) == 0 {
		return false
	}

	run_to_check, _, exact_match := find_possible_run_by_value(rc.run_list, int(n))

	if exact_match {
		return true
	}

	start := run_to_check.start
	end := (start + run_to_check.length) - 1

	return int(n) >= start && int(n) <= end
}

// Array_Container => Bitmap_Container
convert_container_array_to_bitmap :: proc(
	ac: Array_Container,
	allocator := context.allocator,
) -> Bitmap_Container {
	bc := bitmap_container_init(allocator)

	for i in ac.packed_array {
		set_bitmap_container(&bc, i)
	}

	array_container_free(ac)
	return bc
}

// Bitmap_Container => Array_Container
convert_container_bitmap_to_array :: proc(
	bc: Bitmap_Container,
	allocator := context.allocator,
) -> Array_Container {
	ac := array_container_init(allocator)

	for byte, i in bc.bitmap {
		for j in 0..<8 {
			bit_is_set := (byte & (1 << u8(j))) != 0
			if bit_is_set {
				total_i := u16be((i * 8) + j)
				set_array_container(&ac, total_i)
			}
		}
	}

	bitmap_container_free(bc)
	return ac
}

// Run_Container => Bitmap_Container
convert_container_run_to_bitmap :: proc(
	rc: Run_Container,
	allocator := context.allocator,
) -> Bitmap_Container {
	bc := bitmap_container_init(allocator)

	for run in rc.run_list {
		start := run.start
		for i := 0; i < run.length; i += 1 {
			v := u16be(start + i)
			set_bitmap_container(&bc, v)
		}
	}

	run_container_free(rc)
	return bc
}

// Run_Container => Array_Container
convert_container_run_to_array :: proc(
	rc: Run_Container,
	allocator := context.allocator,
) -> Array_Container {
	ac := array_container_init(allocator)

	for run in rc.run_list {
		start := run.start
		for i := 0; i < run.length; i += 1 {
			v := u16be(start + i)
			set_array_container(&ac, v)
		}
	}

	run_container_free(rc)
	return ac
}

// Bitmap_Container => Run_Container
// Ref: https://arxiv.org/pdf/1603.06549 (Page 8)
convert_container_bitmap_to_run :: proc(
	bc: Bitmap_Container,
	allocator := context.allocator
) -> Run_Container {
	run_list := make(Run_List, allocator)

	i := 1
	byte := bc.bitmap[i-1]
	for i <= len(bc.bitmap) {
		if byte == 0b00000000 {
			i += 1

			if i > len(bc.bitmap) {
				break
			}

			byte = bc.bitmap[i-1]
			continue
		}

		j := intrinsics.count_trailing_zeros(byte)
		x := int(j) + 8 * (i - 1)
		byte = byte | (byte - 1)

		for i + 1 <= len(bc.bitmap) && byte == 0b11111111 {
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

		run := Run{start=x, length=(y - x)}
		append(&run_list, run)

		byte = byte & (byte + 1)
	}

	bitmap_container_free(bc)
	return Run_Container{run_list}
}

// Clones any Container to a new version of itself.
clone_container :: proc(
	container: Container,
	allocator := context.allocator,
) -> Container {
	cloned: Container

	switch c in container {
	case Array_Container:
		new_packed_array := slice.clone_to_dynamic(c.packed_array[:], allocator)
		cloned = Array_Container{
			packed_array=new_packed_array,
			cardinality=c.cardinality,
		}
	case Bitmap_Container:
		new_bitmap := slice.clone_to_dynamic(c.bitmap[:], allocator)
		cloned = Bitmap_Container{
			bitmap=new_bitmap,
			cardinality=c.cardinality,
		}
	case Run_Container:
		new_run_list := cast(Run_List)slice.clone_to_dynamic(c.run_list[:], allocator)
		cloned = Run_Container{
			run_list=new_run_list,
		}
	}

	return cloned
}

// Performs an intersection of two Roaring_Bitmap structures.
roaring_intersection :: proc(
	rb1: Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> Roaring_Bitmap {
	rb := roaring_init(allocator)

	for k1, v1 in rb1.index {
		if k1 in rb2.index {
			v2 := rb2.index[k1]

			switch c1 in v1 {
			case Array_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.index[k1] = intersection_array_with_array(c1, c2, allocator)
				case Bitmap_Container:
					rb.index[k1] = intersection_array_with_bitmap(c1, c2, allocator)
				case Run_Container:
					rb.index[k1] = intersection_array_with_run(c1, c2, allocator)
				}
			case Bitmap_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.index[k1] = intersection_array_with_bitmap(c2, c1, allocator)
				case Bitmap_Container:
					rb.index[k1] = intersection_bitmap_with_bitmap(c1, c2, allocator)
				case Run_Container:
					rb.index[k1] = intersection_bitmap_with_run(c1, c2, allocator)
				}
			case Run_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.index[k1] = intersection_array_with_run(c2, c1, allocator)
				case Bitmap_Container:
					rb.index[k1] = intersection_bitmap_with_run(c2, c1, allocator)
				case Run_Container:
					rb.index[k1] = intersection_run_with_run(c1, c2, allocator)
				}
			}
		}
	}

	return rb
}

// Performs a union of two Roaring_Bitmap structures.
roaring_union :: proc(
	rb1: Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> Roaring_Bitmap {
	rb := roaring_init(allocator)

	for k1, v1 in rb1.index {
		// If the container in the first Roaring_Bitmap does not exist in the second,
		// then just copy that container to the new, unioned bitmap.
		if !(k1 in rb2.index) {
			rb.index[k1] = clone_container(v1, allocator)
		}

		if k1 in rb2.index {
			v2 := rb2.index[k1]

			switch c1 in v1 {
			case Array_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.index[k1] = union_array_with_array(c1, c2, allocator)
				case Bitmap_Container:
					rb.index[k1] = union_array_with_bitmap(c1, c2, allocator)
				case Run_Container:
					rb.index[k1] = union_array_with_run(c1, c2, allocator)
				}
			case Bitmap_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.index[k1] = union_array_with_bitmap(c2, c1, allocator)
				case Bitmap_Container:
					rb.index[k1] = union_bitmap_with_bitmap(c1, c2, allocator)
				case Run_Container:
					rb.index[k1] = union_bitmap_with_run(c1, c2, allocator)
				}
			case Run_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.index[k1] = union_array_with_run(c2, c1, allocator)
				case Bitmap_Container:
					rb.index[k1] = union_bitmap_with_run(c2, c1, allocator)
				case Run_Container:
					rb.index[k1] = union_run_with_run(c1, c2, allocator)
				}
			}
		}
	}

	// Lastly, add any containers in the second Roaring_Bitmap that were
	// not present in the first.
	for k2, v2 in rb2.index {
		if !(k2 in rb1.index) {
			rb.index[k2] = clone_container(v2, allocator)
		}
	}

	return rb
}

// The intersection between two array containers is always a new array container.
// We allocate a new array container that has its capacity set to the minimum of
// the cardinalities of the input arrays. When the two input array containers have
// similar cardinalities c1 and c2 (c1/64 < c2 < 64c1), we use a straightforward
// merge algorithm with algorithmic complexity O(c1 + c2), otherwise we use a
// galloping intersection with complexity O(min(c1, c2) log max(c1, c2)). We
// arrived at this threshold (c1/64 < c2 < 64c1) empirically as a reasonable
// choice, but it has not been finely tuned.
intersection_array_with_array :: proc(
	ac1: Array_Container,
	ac2: Array_Container,
	allocator := context.allocator,
) -> Array_Container {
	ac := array_container_init(allocator)

	// Iterate over the smaller container and find all the values that match
	// from the larger. This helps to reduce the no. of binary searches we
	// need to perform.
	if ac1.cardinality < ac2.cardinality {
		for v in ac1.packed_array {
			if is_set_packed_array(ac2, v) {
				set_array_container(&ac, v)
			}
		}
	} else {
		for v in ac2.packed_array {
			if is_set_packed_array(ac1, v) {
				set_array_container(&ac, v)
			}
		}
	}

	// FIXME: Actually do both of these.
	//
	// Ref: https://lemire.me/blog/2019/01/16/faster-intersections-between-sorted-arrays-with-shotgun/
	// Ref: https://softwaredoug.com/blog/2024/05/05/faster-intersect
	//
	// Used to check if the cardinalities differ by less than a factor of 64.
	// For intersections, we use a simple merge (akin to what is done in merge
	// sort) when the two arrays have cardinalities that differ by less than a
	// factor of 64. Otherwise, we use galloping intersections.
	// b1 := (ac1.cardinality / 64) < ac2.cardinality
	// b2 := ac2.cardinality < (ac1.cardinality * 64)
	// // Use the simple merge AKA what we have above.
	// if b1 && b2 {
	// // Use a galloping intersection.
	// } else {
	// }

	// Naive implementation to start. Just binary search every value in c1 in the
	// c2 array. If found, add that value to the new array.

	return ac
}


// For unions, if the sum of the cardinalities of the array containers is 4096 or
// less, we merge the two sorted arrays into a new array container that has its
// capacity set to the sum of the cardinalities of the input arrays. Otherwise, we
// generate an initially empty bitmap container. Though we cannot know whether the
// result will be a bitmap container (i.e., whether the cardinality is larger than
// 4096), as a heuristic, we suppose that it will be so. Iterating through the
// values of both arrays, we set the corresponding bits in the bitmap to 1. Using
// the bitCount function, we compute cardinality, and then convert the bitmap into
// an array container if the cardinality is at most 4096.
union_array_with_array :: proc(
	ac1: Array_Container,
	ac2: Array_Container,
	allocator := context.allocator,
) -> Container {
	if (ac1.cardinality + ac2.cardinality) <= 4096 {
		ac := array_container_init(allocator)
		for v in ac1.packed_array {
			set_array_container(&ac, v)
		}
		// Only add the values from the second array *if* it has not already been added.
		for v in ac2.packed_array {
			if !is_set_packed_array(ac, v) {
				set_array_container(&ac, v)
			}
		}
		return ac
	} else {
		bc := bitmap_container_init(allocator)
		for v in ac1.packed_array {
			set_bitmap_container(&bc, v)
		}
		for v in ac2.packed_array {
			if !is_set_bitmap(bc, v) {
				set_bitmap_container(&bc, v)
			}
		}
		return bc
	}
}

// The intersection between an array and a bitmap container can be computed
// quickly: we iterate over the values in the array container, checking the
// presence of each 16-bit integer in the bitmap container and generating a new
// array container that has as much capacity as the input array container.
intersection_array_with_bitmap :: proc(
	ac: Array_Container,
	bc: Bitmap_Container,
	allocator := context.allocator,
) -> Array_Container {
	nc_ac := array_container_init(allocator)
	for v in ac.packed_array {
		if is_set_bitmap(bc, v) {
			set_array_container(&nc_ac, v)
		}
	}
	return nc_ac
}

// Unions are also efficient: we create a copy of the bitmap and iterate over the
// array, setting the corresponding bits.
union_array_with_bitmap :: proc(
	ac: Array_Container,
	bc: Bitmap_Container,
	allocator := context.allocator,
) -> Bitmap_Container {
	new_container := clone_container(bc, allocator)
	new_bc := new_container.(Bitmap_Container)

	for v in ac.packed_array {
		if !is_set_bitmap(new_bc, v) {
			set_bitmap_container(&new_bc, v)
		}
	}

	return new_bc
}

// Bitmap vs Bitmap: To compute the intersection between two bitmaps, we first
// compute the cardinality of the result using the bitCount function over the
// bitwise AND of the corresponding pairs of words. If the intersection exceeds
// 4096, we materialize a bitmap container by recomputing the bitwise AND between
// the words and storing them in a new bitmap container. Otherwise, we generate a
// new array container by, once again, recomputing the bitwise ANDs, and iterating
// over their 1-bits.
intersection_bitmap_with_bitmap :: proc(
	bc1: Bitmap_Container,
	bc2: Bitmap_Container,
	allocator := context.allocator,
) -> Container {
	count := 0
	for byte1, i in bc1.bitmap {
		byte2 := bc2.bitmap[i]
		res := byte1 & byte2
		count += intrinsics.count_ones(int(res))
	}

	if count > 4096 {
		bc := bitmap_container_init(allocator)
		for byte1, i in bc1.bitmap {
			byte2 := bc2.bitmap[i]
			res := byte1 & byte2
			bc.bitmap[i] = res
			bc.cardinality += intrinsics.count_ones(int(res))
		}
		return bc
	} else {
		ac := array_container_init(allocator)
		for byte1, i in bc1.bitmap {
			byte2 := bc2.bitmap[i]
			res := byte1 & byte2
			for j in 0..<8 {
				bit_is_set := (res & (1 << u8(j))) != 0
				if bit_is_set {
					total_i := u16be((i * 8) + j)
					set_array_container(&ac, total_i)
				}
			}
		}
		return ac
	}
}

// A union between two bitmap containers is straightforward: we execute the
// bitwise OR between all pairs of corresponding words. There are 1024 words in
// each container, so 1024 bitwise OR operations are needed. At the same time, we
// compute the cardinality of the result using the bitCount function on the
// generated words.
union_bitmap_with_bitmap :: proc(
	bc1: Bitmap_Container,
	bc2: Bitmap_Container,
	allocator := context.allocator,
) -> Bitmap_Container {
	bc := bitmap_container_init(allocator)
	for byte, i in bc1.bitmap {
		res := byte | bc2.bitmap[i]
		bc.bitmap[i] = res
		bc.cardinality = intrinsics.count_ones(int(res))
	}
	return bc
}

// "The intersection between a run container and an array container always outputs
// an array container. This choice is easily justified: the result of the
// intersection has cardinality no larger than the array container, and it cannot
// contain more runs than the array container. We can allocate a new array
// container that has its capacity set to the cardinality of the input array
// container. Our algorithm is straightforward. We iterate over the values of the
// array, simultaneously advancing in the run container. Initially, we point at
// the first value in the array container and the first run in the run container.
// While the run ends before the array value, we advance in the run container. If
// the run overlaps the array value, the array value is included in the
// intersection, otherwise it is omitted."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 10)
intersection_array_with_run :: proc(
	ac: Array_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> Array_Container {
	nc_ac := array_container_init(allocator)

	if ac.cardinality == 0 || len(rc.run_list) == 0 {
		return nc_ac
	}

	i := 0
	array_loop: for array_val in ac.packed_array {
		run_loop: for {
			run := rc.run_list[i]
			// If the run contains this array value, set it in the new array containing
			// the intersection and continue at the outer loop with the next array value.
			if int(array_val) >= run.start && int(array_val) < run_end(run) {
				set_array_container(&nc_ac, array_val)
				continue array_loop
			} else {
				i += 1
			}

			// Break out of both loops if we have reached the end.
			if i >= len(rc.run_list) {
				break array_loop
			}
		}
	}

	return nc_ac
}

// "We found that it is often better to predict that the outcome of the union is a
// run container, and to convert the result to a bitmap container, if we must.
// Thus, we follow the heuristic for the union between two run containers,
// effectively treating the array container as a run container where all runs have
// length one. However, once we have computed the union, we must not only check
// whether to convert the result to a bitmap container, but also, possibly, to an
// array container. This check is slightly more expensive, as we must compute the
// cardinality of the result."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 10)
union_array_with_run :: proc(
	ac: Array_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> Container {
	new_rc := clone_container(rc).(Run_Container)

	for v in ac.packed_array {
		set_run_container(&new_rc, v)
	}

	return convert_container_optimal(new_rc)
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
intersection_bitmap_with_run :: proc(
	bc: Bitmap_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> Container {
	if container_cardinality(rc) <= 4096 {
		nc_ac := array_container_init(allocator)
		for run in rc.run_list {
			for i := run.start; i < run_end(run); i += 1 {
				if is_set_bitmap(bc, u16be(i)) {
					set_array_container(&nc_ac, u16be(i))	
				}
			}
		}
		return nc_ac
	} else {
		new_bc := clone_container(bc, allocator).(Bitmap_Container)

		// Set the complement of the Run_List to be zero.
		for run, i in rc.run_list {
			if i == 0 && run.start > 0 {
				unset_range_of_bits_in_bitmap_container(&new_bc, 0, run.length)
			} else if i > 0 {
				prev_run := rc.run_list[i - 1]
				complement_start := run.start - prev_run.start + 1
				complement_length := run.start - complement_start
				unset_range_of_bits_in_bitmap_container(&new_bc, complement_start, complement_length)
			}
		}

		// Set any remaining bits after the last Run to be 0.
		last_run := rc.run_list[len(rc.run_list) - 1]
		unset_start := run_end(last_run) + 1
		unset_length := (len(bc.bitmap) * 8) - unset_start
		unset_range_of_bits_in_bitmap_container(&new_bc, unset_start, unset_length)

		// Determine the cardinality.
		acc := 0
		for byte in new_bc.bitmap {
			acc += intrinsics.count_ones(int(byte))
		}
		new_bc.cardinality = acc

		// Convert down to a Array_Container if needed.
		if new_bc.cardinality <= 4096 {
			return convert_container_bitmap_to_array(new_bc, allocator)
		} else {
			return new_bc
		}
	}
}

// "The union between a run container and a bitmap container is computed by first
// cloning the bitmap container. We then set to one all bits corresponding to the
// integers in the run container, using fast bitwise OR operations (see again
// Algorithm 3)."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 11)
union_bitmap_with_run :: proc(
	bc: Bitmap_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> Bitmap_Container {
	new_bc := clone_container(bc, allocator).(Bitmap_Container)

	for run in rc.run_list {
		set_range_of_bits_in_bitmap_container(&new_bc, run.start, run.length)
	}

	new_bc.cardinality = bitmap_container_calculate_cardinality(new_bc)
	return new_bc
}

// Sets a range of bits from 0 to 1 in a Bitmap_Container bitmap.
// Ref: https://arxiv.org/pdf/1603.06549 (Page 11)
set_range_of_bits_in_bitmap_container :: proc(bc: ^Bitmap_Container, start: int, length: int) {
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
unset_range_of_bits_in_bitmap_container :: proc(bc: ^Bitmap_Container, start: int, length: int) {
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

// "When computing the intersection between two run containers, we first produce a
// new run container by a simple intersection algorithm. This new run container
// has its capacity set to the sum of the number of runs in both input containers.
// The algorithm starts by considering the first run, in each container. If they
// do not overlap, we advance in the container where the run occurs earlier until
// they do overlap, or we run out of runs in one of the containers. When we run
// out of runs in either container, the algorithm terminates. When two runs
// overlap, we always output their intersection. If the two runs end at the same
// value, then we advance in the two run containers. Otherwise, we advance only in
// the run container that ends first. Once we have computed the answer, after
// exhausting the runs in at least one container, we check whether the run
// container should be converted to either a bitmap (if it has too many runs) or
// to an array container (if its cardinality is too small compared to the number
// of runs)."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 10)
intersection_run_with_run :: proc(
	rc1: Run_Container,
	rc2: Run_Container,
	allocator := context.allocator,
) -> Container {
	new_rc := run_container_init(allocator)
	i := 0
	j := 0

	outer: for {
		if i >= len(rc1.run_list) || j >= len(rc2.run_list) {
			break outer
		}

		run1 := rc1.run_list[i]
		run2 := rc2.run_list[j]

		if runs_overlap(run1, run2) {
			overlap_start, overlap_end := run_overlapping_range(run1, run2)
			for n in overlap_start..=overlap_end {
				set_run_container(&new_rc, u16be(n))
			}

			if run_end(run1) < run_end(run2) {
				i += 1
			} else if run_end(run2) < run_end(run1) {
				j += 1
			} else {
				i += 1
				j += 1
			}
		} else {
			if run1.start < run2.start {
				i += 1
			} else {
				j += 1
			}
		}
	}

	return convert_container_optimal(new_rc)
}

// "The union algorithm is also conceptually simple. We create a new, initially
// empty, run container that has its capacity set to the sum of the number of runs
// in both input containers. We iterate over the runs, starting from the first run
// in each container. Each time, we pick a run that has a minimal starting point.
// We append it to the output either as a new run, or as an extension of the
// previous run. We then advance in the container where we picked the run. Once a
// container has no more runs, all runs remaining in the other container are
// appended to the answer. After we have computed the resulting run container, we
// convert the run container into a bitmap container if too many runs were
// created. Checking whether such a conversion is needed is fast, since it can be
// decided only by checking the number of runs. There is no need to consider
// conversion to an array container, because every run present in the original
// inputs is either present in its entirety, or as part of an even larger run.
// Thus the average run length (essentially our criterion for conversion) is at
// least as large as in the input run containers."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 10)
union_run_with_run :: proc(
	rc1: Run_Container,
	rc2: Run_Container,
	allocator := context.allocator,
) -> Container {
	new_rc := run_container_init(allocator)

	// FIXME: Can any of this be optimized?
	for run in rc1.run_list {
		for i := run.start; i < run_end(run); i += 1 {
			set_run_container(&new_rc, u16be(i))
		}
	}

	// FIXME: Can any of this be optimized?
	for run in rc2.run_list {
		for i := run.start; i < run_end(run); i += 1 {
			set_run_container(&new_rc, u16be(i))
		}
	}


	return convert_container_optimal(new_rc)
}

// Checks if two Run structs overlap at all.
runs_overlap :: proc(r1: Run, r2: Run) -> bool {
	start1 := r1.start
	start2 := r2.start
	end1 := run_end(r1)
	end2 := run_end(r2)

	return start1 < end2 && start2 < end1
}

// Finds the range (inclusive at both ends) that two Run
// structs are overlapping at.
run_overlapping_range :: proc(r1: Run, r2: Run) -> (start: int, end: int) {
	if !runs_overlap(r1, r2) {
		return -1, -1
	}

	start1 := r1.start
	end1 := r1.start + r1.length - 1
	start2 := r2.start
	end2 := r2.start + r2.length - 1

	// Max of start1 and start2
	// Min of end1 and end2
	return builtin.max(start1, start2), builtin.min(end1, end2)
}

// Counts the no. of runs in a bitmap (eg., Bitmap_Container).
// Ref: https://arxiv.org/pdf/1603.06549 (Page 7, Algorithm 1)
bitmap_container_count_runs :: proc(bc: Bitmap_Container) -> (count: int) {
	for i in 0..<(len(bc.bitmap) - 1) {
		byte := bc.bitmap[i]
		count += intrinsics.count_ones(int(byte << 1) &~ int(byte)) + int((byte >> 7) &~ bc.bitmap[i + 1])
	}

	byte := bc.bitmap[len(bc.bitmap) - 1]
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
should_convert_container_bitmap_to_run :: proc(bc: Bitmap_Container) -> bool {
	run_count := bitmap_container_count_runs(bc)

	// "If the run container has cardinality no more than 4096, then the number of
	// runs must be less than half the cardinality."
	// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
	return run_count < (bc.cardinality / 2)
}

// Converts a given container into its optimal representation, using a
// variety of heuristics.
convert_container_optimal :: proc(container: Container, allocator := context.allocator) -> Container {
	optimal: Container

	switch c in container {
	case Array_Container:
		if len(c.packed_array) <= 4096 {
			optimal = c
		}

		bc := convert_container_array_to_bitmap(c, allocator)
		if should_convert_container_bitmap_to_run(bc) {
			optimal = convert_container_bitmap_to_run(bc, allocator)
		} else {
			optimal = bc
		}
	case Bitmap_Container:
		if c.cardinality <= 4096 {
			optimal = convert_container_bitmap_to_array(c, allocator)	
		}

		if should_convert_container_bitmap_to_run(c) {
			optimal = convert_container_bitmap_to_run(c, allocator)
		} else {
			optimal = c
		}
	case Run_Container:
		cardinality := run_container_calculate_cardinality(c)

		// "If the run container has cardinality greater than 4096 values, then it
		// must contain no more than ⌈(8192 − 2)/4⌉ = 2047 runs."
		// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
		if cardinality > 4096 {
			if len(c.run_list) <= 2047 {
				optimal = c
			} else {
				optimal = convert_container_run_to_bitmap(c)
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
				optimal = convert_container_run_to_array(c)
			}
		}
	}

	return optimal
}

// Calculates the end position of the given Run in the container (exclusive).
run_end :: proc(run: Run) -> int {
	return run.start + run.length
}

// Finds the cardinality of a Array_Container.
array_container_calculate_cardinality :: proc(ac: Array_Container) -> int {
	return len(ac.packed_array)
}

// Finds the cardinality of a Bitmap_Container by finding all the set bits.
bitmap_container_calculate_cardinality :: proc(bc: Bitmap_Container) -> (acc: int) {
	for byte in bc.bitmap {
		if byte != 0 {
			acc += intrinsics.count_ones(int(byte))
		}
	}

	return acc
}

// Finds the cardinality of a Run_Container by summing the length of each run.
run_container_calculate_cardinality :: proc(rc: Run_Container) -> (acc: int) {
	rl := rc.run_list

	if len(rl) == 0 {
		return 0
	}

	for run in rc.run_list {
		acc += run.length
	}

	return acc
}

// "Thus, when first creating a Roaring bitmap, it is usually made of array and
// bitmap containers. Runs are not compressed. Upon request, the storage of the
// Roaring bitmap can be optimized using the runOptimize function. This triggers a
// scan through the array and bitmap containers that converts them, if helpful, to
// run containers. In a given application, this might be done prior to storing the
// bitmaps as immutable objects to be queried. Run containers may also arise from
// calling a function to add a range of values."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
run_optimize :: proc(rb: ^Roaring_Bitmap) {
	index := &rb.index
	for key, container in index {
		index[key] = convert_container_optimal(container)
	}
}

main :: proc() {
	fmt.println("Hello, world!")
}
