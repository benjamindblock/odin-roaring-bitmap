package roaring

import "base:builtin"
import "base:intrinsics"
import "base:runtime"
import "core:fmt"
import "core:io"
import "core:mem"
import "core:os"
import "core:slice"

MAX_RUNS_PERMITTED :: 2047

MAX_ARRAY_LENGTH :: 4096

// NOTE: The implementation of the bitmap right now uses 8192 8-bit words
// instead of 1024 64-bit words as specified in some roaring papers.
BYTES_PER_BITMAP :: 8192

// Constants for serializing and deserializing.
SERIAL_COOKIE_NO_RUNCONTAINER :: 12346
SERIAL_COOKIE :: 12347
NO_OFFSET_THRESHOLD :: 4

Roaring_Error :: union {
	Already_Set_Error,
	Not_Set_Error,
	Parse_Error,
	Parse_Endian_Error,
	runtime.Allocator_Error,
	os.Error,
	io.Error,
}

Already_Set_Error :: struct {
	value: u32,
}

Not_Set_Error :: struct {
	value: u32,
}

Parse_Error :: struct {}
Parse_Endian_Error :: struct {}

// "An array container is an object containing a counter keeping track of the
// number of integers, followed by a packed array of sorted 16-bit unsigned
// integers. It can be serialized as an array of 16-bit values."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 5)
Array_Container :: struct {
	packed_array: [dynamic]u16,
	cardinality: int,
}

// "A bitmap container is an object made of 1024 64-bit words (using 8 kB)
// representing an uncompressed bitmap, able to store all sets of 16-bit integers.
// The container can be serialized as an array of 64-bit words. We also maintain a
// counter to record how many bits are set to 1."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 5)
//
// NOTE: Using 8192 8-bit words instead of 1024 64-bit words.
Bitmap_Container :: struct {
	bitmap: [8192]u8,
	cardinality: int,
}

// "Unlike an array or bitmap container, a run container does not keep track of its
// cardinality; its cardinality can be computed on the fly by summing the lengths
// of the runs. In most applications, we expect the number of runs to be often
// small: the computation of the cardinality should not be a bottleneck."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
//
// Length is the real length - 1 so that we can fit it into a u16
Run :: struct {
	start: u16,
	length: u16,
}

Run_List :: distinct [dynamic]Run

Run_Container :: struct {
	run_list: Run_List,
}

Container :: union {
	Array_Container,
	Bitmap_Container,
	Run_Container,
}

// Sorted index to the map.
Container_Index :: distinct [dynamic]u16

// Repository of every container.
Container_Map :: distinct map[u16]Container

Roaring_Bitmap :: struct {
	cindex: Container_Index,
	containers: Container_Map,
	allocator: mem.Allocator,
}

init :: proc(
	allocator := context.allocator
) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	cindex: Container_Index
	cindex, err = make(Container_Index, 0, allocator)
	if err != runtime.Allocator_Error.None {
		return rb, err
	}

	containers: Container_Map
	containers, err = make(Container_Map, 0, allocator)
	if err != runtime.Allocator_Error.None {
		return rb, err
	}

	rb = Roaring_Bitmap{
		cindex = cindex,
		containers=containers,
		allocator=allocator,
	}
	return rb, nil
}

destroy :: proc(rb: ^Roaring_Bitmap) {
	for i, _ in rb.containers {
		free_at(rb, i)
	}

	clear(&rb.containers)
	delete(rb.containers)
	clear(&rb.cindex)
	delete(rb.cindex)

	assert(len(rb.cindex) == 0, "CIndex should be zero after freeing!")
	assert(len(rb.containers) == 0, "Containers should be gone after freeing!")
}

clone :: proc(
	rb: Roaring_Bitmap,
	allocator := context.allocator
) -> (new_rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	new_rb = init(allocator) or_return
	for key, container in rb.containers {
		new_rb.containers[key] = container_clone(container, allocator) or_return
		cindex_ordered_insert(&new_rb, key)
	}
	return new_rb, nil
}

@(private)
cindex_ordered_remove :: proc(rb: ^Roaring_Bitmap, n: u16) {
	i, found := slice.binary_search(rb.cindex[:], n)
	if !found {
		return
	}
	ordered_remove(&rb.cindex, i)
}

@(private)
cindex_ordered_insert :: proc(rb: ^Roaring_Bitmap, n: u16) -> (ok: bool, err: runtime.Allocator_Error) {
	i, found := slice.binary_search(rb.cindex[:], n)
	if found {
		return true, nil
	}
	inject_at(&rb.cindex, i, n) or_return

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Removes a container and its position in the index from the Roaring_Bitmap.
@(private)
free_at :: proc(rb: ^Roaring_Bitmap, i: u16) {
	container := rb.containers[i]
	container_destroy(container)
	delete_key(&rb.containers, i)
	cindex_ordered_remove(rb, i)
	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
}

// Returns all of the set values of a Roaring_Bitmap as an array.
to_array :: proc(rb: Roaring_Bitmap, allocator := context.allocator) -> [dynamic]u32 {
	rb := rb
	iterator := make_iterator(&rb)

	acc := make([dynamic]u32, allocator)
	for v in iterate_set_values(&iterator) {
		append(&acc, v)
	}
	return acc
}

// Prints statistics on the Roaring_Bitmap.
print_stats :: proc(rb: Roaring_Bitmap) {
	ac: int
	bmc: int
	rcc: int

	for _, container in rb.containers {
		switch c in container {
		case Array_Container:
			ac += 1
		case Bitmap_Container:
			bmc += 1
		case Run_Container:
			rcc += 1
		}
	}

	fmt.println("# of containers", len(rb.cindex))
	fmt.println("Array_Container:", ac)
	fmt.println("Bitmap_Container", bmc)
	fmt.println("Run_Container:", rcc)
	fmt.println("Size in bytes:", size_in_bytes(rb))
}

// Prints statistics on the Roaring_Bitmap.
size_in_bytes :: proc(rb: Roaring_Bitmap) -> (size: int) {
	for _, container in rb.containers {
		switch c in container {
		case Array_Container:
			size += size_of(c.packed_array)
		case Bitmap_Container:
			size += size_of(c.bitmap)
		case Run_Container:
			size += size_of(c.run_list)
		}
	}
	return size
}

// Adds a value to the Roaring_Bitmap. If a container doesn’t already exist
// for the value, then create a new array container and add it to the index
// before setting the value.
//
// This method does not care if that value is already set or not, it will
// set the value anyways. If you *do* care, use `strict_add` to fail when
// a value is already set in the Roaring_Bitmap.
add :: proc(
	rb: ^Roaring_Bitmap,
	n: u32,
) -> (ok: bool, err: runtime.Allocator_Error) {
	i := most_significant(n)
	j := least_significant(n)

	if !(i in rb.containers) {
		rb.containers[i] = array_container_init(rb.allocator) or_return
		cindex_ordered_insert(rb, i)
		return add(rb, n)
	}

	container := &rb.containers[i]
	switch &c in container {
	case Array_Container:
		rb.containers[i] = array_container_add(&c, j, rb.allocator) or_return
	case Bitmap_Container:
		bitmap_container_add(&c, j)
	case Run_Container:
		rb.containers[i] = run_container_add(&c, j) or_return
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Sets multiple values in a Roaring_Bitmap.
add_many :: proc(
	rb: ^Roaring_Bitmap,
	nums: ..u32,
) -> (ok: bool, err: runtime.Allocator_Error) {
	for n in nums {
		add(rb, n) or_return
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Adds a number to the bitmap, but fails if that value is already set.
strict_add :: proc(rb: ^Roaring_Bitmap, n: u32) -> (ok: bool, err: Roaring_Error) {
	if contains(rb^, n) {
		return false, Already_Set_Error{n}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return add(rb, n)
}

// Sets multiple values in a Roaring_Bitmap, but checks that none of them
// exist first. If at least one of them does, an error is raised before
// and values are set.
strict_add_many :: proc(rb: ^Roaring_Bitmap, nums: ..u32) -> (ok: bool, err: Roaring_Error) {
	for n in nums {
		if contains(rb^, n) {
			return false, Already_Set_Error{n}
		}
	}

	for n in nums {
		add(rb, n) or_return
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Removes a value from the Roaring_Bitmap. This method not care if that value is
// actually set or not. Use `strict_remove` you do care and want to fail.
remove :: proc(
	rb: ^Roaring_Bitmap,
	n: u32,
) -> (ok: bool, err: runtime.Allocator_Error) {
	i := most_significant(n)
	j := least_significant(n)

	if !(i in rb.containers) {
		return true, nil
	}

	container := &rb.containers[i]
	switch &c in container {
	case Array_Container:
		array_container_remove(&c, j) or_return
	case Bitmap_Container:
		rb.containers[i] = bitmap_container_remove(&c, j, rb.allocator) or_return
	case Run_Container:
		rb.containers[i] = run_container_remove(&c, j, rb.allocator) or_return
	}

	// If we have removed the last element(s) in a container, remove the
	// container + key.
	container = &rb.containers[i]
	if container_get_cardinality(container^) == 0 {
		free_at(rb, i)
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

remove_many :: proc(
	rb: ^Roaring_Bitmap,
	nums: ..u32,
) -> (ok: bool, err: runtime.Allocator_Error) {
	for n in nums {
		remove(rb, n) or_return
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Removes a number from the bitmap, but fails if that value is *not* actually set.
strict_remove :: proc(
	rb: ^Roaring_Bitmap,
	n: u32,
) -> (ok: bool, err: Roaring_Error) {
	if !contains(rb^, n) {
		return false, Not_Set_Error{n}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return remove(rb, n)
}

// Removes numbers from the bitmap, but fails if any value attempting to be set
// is *not* actually set.
strict_remove_many :: proc(
	rb: ^Roaring_Bitmap,
	nums: ..u32,
) -> (ok: bool, err: Roaring_Error) {
	for n in nums {
		if !contains(rb^, n) {
			return false, Not_Set_Error{n}
		}
	}

	for n in nums {
		remove(rb, n) or_return
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Flips all the bits from a start range (inclusive) to end (inclusive) in a Roaring_Bitmap
// and returns the result as a new Roaring_Bitmap.
flip :: proc(
	rb: Roaring_Bitmap,
	start: u32,
	end: u32,
) -> (new_rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	new_rb = clone(rb) or_return
	flip_inplace(&new_rb, start, end) or_return

	assert(len(new_rb.cindex) == len(new_rb.containers), "Containers and CIndex are out of sync!")
	return new_rb, nil
}

// Flips all the bits from a start range (inclusive) to end (inclusive) in a Roaring_Bitmap.
flip_inplace :: proc(
	rb: ^Roaring_Bitmap,
	start: u32,
	end: u32,
) -> (ok: bool, err: runtime.Allocator_Error) {
	start_be := u32(start)
	start_i := most_significant(start_be)
	start_j := least_significant(start_be)

	end_be := u32(end)
	end_i := most_significant(end_be)
	end_j := least_significant(end_be)

	// Flipping a range within a single container.
	if start_i == end_i {
		container_flip(rb, start_i, start_j, end_j)
		return
	}

	for i in start_i..=end_i {
		// If we are at the first container, start flipping from the beginning of the range
		// until the end of the container.
		if i == start_i {
			container_flip(rb, i, start_j, 65535)
		// If we are at the last container, start flipping from 0 until the end of the range.
		} else if i == end_i {
			container_flip(rb, i, 0, end_j)
		// Otherwise we are in an intermediary container, in which case flip the entire thing.
		} else {
			container_flip(rb, i, 0, 65535)
		}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Add the value if it is not already present, otherwise remove it.
flip_at :: proc(rb: ^Roaring_Bitmap, n: u32) {
	if contains(rb^, n) {
		remove(rb, n)
	} else {
		add(rb, n)
	}
}

// Estimate of the memory usage of this data structure.
estimate_size_in_bytes :: proc(rb: Roaring_Bitmap) -> (size: int) {
	size += size_of(rb.cindex)

	for _, container in rb.containers {
		switch c in container {
		case Array_Container:
			size += size_of(c.packed_array)
		case Bitmap_Container:
			size += size_of(c.bitmap)
		case Run_Container:
			size += size_of(c.run_list)
		}
	}

	return size
}

has_run_compression :: proc(rb: Roaring_Bitmap) -> bool {
	for _, container in rb.containers {
		switch c in container {
		case Array_Container, Bitmap_Container:
			continue
		case Run_Container:
			return true
		}
	}

	return false
}

// To check if an integer N exists, get N’s 16 most significant bits (N / 2^16)
// and use it to find N’s corresponding container in the Roaring bitmap.
// If the container doesn’t exist, then N is not in the Roaring bitmap.
// Checking for existence in array and bitmap containers works differently:
//   Bitmap: check if the bit at N % 2^16 is set.
//   Array: use binary search to find N % 2^16 in the sorted array.
@(require_results)
contains :: proc(rb: Roaring_Bitmap, n: u32) -> (found: bool) {
	i := most_significant(n)
	j := least_significant(n)

	if !(i in rb.containers) {
		return false
	}

	container := rb.containers[i]
	switch c in container {
	case Array_Container:
		found = array_container_contains(c, j)
	case Bitmap_Container:
		found = bitmap_container_contains(c, j)
	case Run_Container:
		found = run_container_contains(c, j)
	}

	return found
}

// Gets the value (0 or 1) of the N-th value.
@(require_results)
select :: proc(rb: Roaring_Bitmap, n: u32) -> int {
	if contains(rb, n) {
		return 1
	} else {
		return 0
	}
}

// Performs an AND between of two Roaring_Bitmap structures and returns
// a new Roaring_Bitmap containing the result.
and :: proc(
	rb1: Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	rb = init(allocator) or_return

	for k1, v1 in rb1.containers {
		if k1 in rb2.containers {
			v2 := rb2.containers[k1]

			switch c1 in v1 {
			case Array_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.containers[k1] = array_container_and_array_container(c1, c2, allocator) or_return
				case Bitmap_Container:
					rb.containers[k1] = array_container_and_bitmap_container(c1, c2, allocator) or_return
				case Run_Container:
					rb.containers[k1] = array_container_and_run_container(c1, c2, allocator) or_return
				}
			case Bitmap_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.containers[k1] = array_container_and_bitmap_container(c2, c1, allocator) or_return
				case Bitmap_Container:
					rb.containers[k1] = bitmap_container_and_bitmap_container(c1, c2, allocator) or_return
				case Run_Container:
					rb.containers[k1] = bitmap_container_and_run_container(c1, c2, allocator) or_return
				}
			case Run_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.containers[k1] = array_container_and_run_container(c2, c1, allocator) or_return
				case Bitmap_Container:
					rb.containers[k1] = bitmap_container_and_run_container(c2, c1, allocator) or_return
				case Run_Container:
					rb.containers[k1] = run_container_and_run_container(c1, c2, allocator) or_return
				}
			}

			cindex_ordered_insert(&rb, k1)
		}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return rb, nil
}

// Performs an AND between two bitmaps and stores the results inside the
// first bitmap.
//
// TODO: The underlying methods *do not* modify the containers in place,
// so here we are changing data in-place, but still requiring intermediate
// allocations to take place. Is there a way to do it all in place?
and_inplace :: proc(
	rb1: ^Roaring_Bitmap,
	rb2: Roaring_Bitmap,
) -> (ok: bool, err: runtime.Allocator_Error) {
	for k1, container1 in rb1.containers {
		if k1 in rb2.containers {
			container2 := rb2.containers[k1]
			switch c1 in container1 {
			case Array_Container:
				switch c2 in container2 {
				case Array_Container:
					rb1.containers[k1] = array_container_and_array_container(c1, c2, rb1.allocator) or_return
				case Bitmap_Container:
					rb1.containers[k1] = array_container_and_bitmap_container(c1, c2, rb1.allocator) or_return
				case Run_Container:
					rb1.containers[k1] = array_container_and_run_container(c1, c2, rb1.allocator) or_return
				}
			case Bitmap_Container:
				switch c2 in container2 {
				case Array_Container:
					rb1.containers[k1] = array_container_and_bitmap_container(c2, c1, rb1.allocator) or_return
				case Bitmap_Container:
					rb1.containers[k1] = bitmap_container_and_bitmap_container(c1, c2, rb1.allocator) or_return
				case Run_Container:
					rb1.containers[k1] = bitmap_container_and_run_container(c1, c2, rb1.allocator) or_return
				}
			case Run_Container:
				switch c2 in container2 {
				case Array_Container:
					rb1.containers[k1] = array_container_and_run_container(c2, c1, rb1.allocator) or_return
				case Bitmap_Container:
					rb1.containers[k1] = bitmap_container_and_run_container(c2, c1, rb1.allocator) or_return
				case Run_Container:
					rb1.containers[k1] = run_container_and_run_container(c1, c2, rb1.allocator) or_return
				}
			}
		}
	}

	// Remove any empty containers after performing the bitwise AND.
	for key, container in rb1.containers {
		if !(key in rb2.containers) || container_get_cardinality(container) == 0 {
			free_at(rb1, key)
		}
	}

	assert(len(rb1.cindex) == len(rb1.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Performs an ANDNOT (&~) between of two Roaring_Bitmap structures and returns
// a new Roaring_Bitmap containing the result.
//
// TODO: Optimize this with native ANDNOT operations for each variation.
// Right now we cheat a by converting to bitmap containers and then performing
// the ANDNOT with simple binary operations.
andnot :: proc(
	rb1: Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	rb = init(allocator) or_return

	for k1, container1 in rb1.containers {
		if !(k1 in rb2.containers) {
			rb.containers[k1] = container_clone(container1, allocator) or_return
			cindex_ordered_insert(&rb, k1)
			continue
		}

		bc1 := container_clone_to_bitmap(container1) or_return
		bc2 := container_clone_to_bitmap(rb2.containers[k1]) or_return
		res := bitmap_container_andnot_bitmap_container(bc1, bc2, allocator) or_return

		if container_get_cardinality(res) > 0 {
			rb.containers[k1] = res
			cindex_ordered_insert(&rb, k1)
		}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return rb, nil
}

// Performs an ANDNOT (&~) between of two Roaring_Bitmap in-place, meaning
// the results will be present in the first bitmap provided.
//
// TODO: Optimize this with native ANDNOT operations for each variation.
// Right now we cheat a by converting to bitmap containers and then performing
// the ANDNOT with simple binary operations.
andnot_inplace :: proc(
	rb1: ^Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (err: runtime.Allocator_Error) {
	for k1, container1 in rb1.containers {
		// Always delete the original container from the first Roaring_Bitmap as we will
		// either be: 
		// 1. Replacing it with a new ANDNOT'ed container
		// 2. Ignoring it because it does not exist in the second Roaring_Bitmap
		//
		// The last loop at the end will ensure that we update the cindex appropriately.
		defer container_destroy(container1)

		if k1 in rb2.containers {
			bc1 := container_clone_to_bitmap(container1) or_return
			defer container_destroy(bc1)

			bc2 := container_clone_to_bitmap(rb2.containers[k1]) or_return
			defer container_destroy(bc2)

			res := bitmap_container_andnot_bitmap_container(bc1, bc2, allocator) or_return
			rb1.containers[k1] = res
		}
	}

	// Remove any empty containers after performing the bitwise ANDNOT.
	for key, container in rb1.containers {
		if container_get_cardinality(container) == 0 {
			free_at(rb1, key)
		}
	}

	assert(len(rb1.cindex) == len(rb1.containers), "Containers and CIndex are out of sync!")
	return nil
}

// Performs an OR (eg., union) of two Roaring_Bitmap structures and returns
// a new Roaring_Bitmap holding the results.
or :: proc(
	rb1: Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	rb = init(allocator) or_return

	for k1, v1 in rb1.containers {
		// If the container in the first Roaring_Bitmap does not exist in the second,
		// then just copy that container to the new, unioned bitmap.
		if !(k1 in rb2.containers) {
			rb.containers[k1] = container_clone(v1, allocator) or_return
			cindex_ordered_insert(&rb, k1)
		}

		if k1 in rb2.containers {
			v2 := rb2.containers[k1]

			switch c1 in v1 {
			case Array_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.containers[k1] = array_container_or_array_container(c1, c2, allocator) or_return
				case Bitmap_Container:
					rb.containers[k1] = array_container_or_bitmap_container(c1, c2, allocator) or_return
				case Run_Container:
					rb.containers[k1] = array_container_or_run_container(c1, c2, allocator) or_return
				}
			case Bitmap_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.containers[k1] = array_container_or_bitmap_container(c2, c1, allocator) or_return
				case Bitmap_Container:
					rb.containers[k1] = bitmap_container_or_bitmap_container(c1, c2)
				case Run_Container:
					rb.containers[k1] = bitmap_container_or_run_container(c1, c2, allocator) or_return
				}
			case Run_Container:
				switch c2 in v2 {
				case Array_Container:
					rb.containers[k1] = array_container_or_run_container(c2, c1, allocator) or_return
				case Bitmap_Container:
					rb.containers[k1] = bitmap_container_or_run_container(c2, c1, allocator) or_return
				case Run_Container:
					rb.containers[k1] = run_container_or_run_container(c1, c2, allocator) or_return
				}
			}
		}

		cindex_ordered_insert(&rb, k1)
	}

	// Lastly, add any containers in the second Roaring_Bitmap that were
	// not present in the first.
	for k2, v2 in rb2.containers {
		if !(k2 in rb1.containers) {
			rb.containers[k2] = container_clone(v2, allocator) or_return
			cindex_ordered_insert(&rb, k2)
		}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return rb, nil
}

// Performs an OR between two bitmaps and stores the results inside the
// first bitmap.
//
// TODO: The underlying methods *do not* modify the containers in place,
// so here we are changing data in-place, but still requiring intermediate
// allocations to take place. Is there a way to do it all in place?
or_inplace :: proc(
	rb1: ^Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (ok: bool, err: runtime.Allocator_Error) {
	for k1, v1 in rb1.containers {
		// If the container in the first Roaring_Bitmap does not exist in the second,
		// then just skip processing this. The result will be the same.
		if !(k1 in rb2.containers) {
			continue
		}

		// Always delete the original container from the first Roaring_Bitmap because
		// it will be replaced with the new OR'ed container
		defer container_destroy(v1)

		v2 := rb2.containers[k1]
		switch c1 in v1 {
		case Array_Container:
			switch c2 in v2 {
			case Array_Container:
				rb1.containers[k1] = array_container_or_array_container(c1, c2, allocator) or_return
			case Bitmap_Container:
				rb1.containers[k1] = array_container_or_bitmap_container(c1, c2, allocator) or_return
			case Run_Container:
				rb1.containers[k1] = array_container_or_run_container(c1, c2, allocator) or_return
			}
		case Bitmap_Container:
			switch c2 in v2 {
			case Array_Container:
				rb1.containers[k1] = array_container_or_bitmap_container(c2, c1, allocator) or_return
			case Bitmap_Container:
				rb1.containers[k1] = bitmap_container_or_bitmap_container(c1, c2)
			case Run_Container:
				rb1.containers[k1] = bitmap_container_or_run_container(c1, c2, allocator) or_return
			}
		case Run_Container:
			switch c2 in v2 {
			case Array_Container:
				rb1.containers[k1] = array_container_or_run_container(c2, c1, allocator) or_return
			case Bitmap_Container:
				rb1.containers[k1] = bitmap_container_or_run_container(c2, c1, allocator) or_return
			case Run_Container:
				rb1.containers[k1] = run_container_or_run_container(c1, c2, allocator) or_return
			}
		}
	}

	// Lastly, add any containers in the second Roaring_Bitmap that are not present in the first.
	for k2, v2 in rb2.containers {
		if !(k2 in rb1.containers) {
			rb1.containers[k2] = container_clone(v2, allocator) or_return
			cindex_ordered_insert(rb1, k2)
		}
	}

	assert(len(rb1.cindex) == len(rb1.containers), "Containers and CIndex are out of sync!")
	return true, nil
}

// Performs an XOR (~) between of two Roaring_Bitmap in-place, meaning
// the results will be present in the first bitmap provided.
//
// TODO: Optimize this with native XOR operations for each variation.
// Right now we cheat a by converting to bitmap containers and then performing
// the XOR with simple binary operations.
xor :: proc(
	rb1: Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {
	rb = init(allocator) or_return

	for k1, container1 in rb1.containers {
		if k1 in rb2.containers {
			bc1 := container_clone_to_bitmap(container1) or_return
			defer container_destroy(bc1)

			bc2 := container_clone_to_bitmap(rb2.containers[k1]) or_return
			defer container_destroy(bc2)

			res := bitmap_container_xor_bitmap_container(bc1, bc2, allocator) or_return
			if container_get_cardinality(res) > 0 {
				rb.containers[k1] = res
				cindex_ordered_insert(&rb, k1)
			}
		} else {
			// If this container *is not* present in the second bitmap, then we can
			// add the entire thing.
			rb.containers[k1] = container_clone(container1, allocator) or_return
			cindex_ordered_insert(&rb, k1)
		}
	}

	// Add any missing containers from the second bitmap.
	for k2, container2 in rb2.containers {
		if !(k2 in rb1.containers) {
			rb.containers[k2] = container_clone(container2, allocator) or_return
			cindex_ordered_insert(&rb, k2)
		}
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return rb, nil
}

// Performs an XOR (~) between of two Roaring_Bitmap in-place, meaning
// the results will be present in the first bitmap provided.
//
// TODO: Optimize this with native XOR operations for each variation.
// Right now we cheat a by converting to bitmap containers and then performing
// the XOR with simple binary operations.
xor_inplace :: proc(
	rb1: ^Roaring_Bitmap,
	rb2: Roaring_Bitmap,
	allocator := context.allocator,
) -> (err: runtime.Allocator_Error) {
	for k1, container1 in rb1.containers {
		if k1 in rb2.containers {
			bc1 := container_clone_to_bitmap(container1) or_return
			bc2 := container_clone_to_bitmap(rb2.containers[k1]) or_return
			res := bitmap_container_xor_bitmap_container(bc1, bc2, allocator) or_return
			rb1.containers[k1] = res
		}
	}

	// Add any missing containers from the second bitmap.
	for k2, container2 in rb2.containers {
		if !(k2 in rb1.containers) {
			rb1.containers[k2] = container_clone(container2, allocator) or_return
			cindex_ordered_insert(rb1, k2)
		}
	}

	// Remove any empty containers after performing the bitwise XOR.
	for key, container in rb1.containers {
		if container_get_cardinality(container) == 0 {
			free_at(rb1, key)
		}
	}

	assert(len(rb1.cindex) == len(rb1.containers), "Containers and CIndex are out of sync!")
	return nil
}

// Returns the overall cardinality for the Roaring_Bitmap.
get_cardinality :: proc(rb: Roaring_Bitmap) -> (cardinality: int) {
	for _, container in rb.containers {
		cardinality += container_get_cardinality(container)
	}
	return cardinality
}

// "Thus, when first creating a Roaring bitmap, it is usually made of array and
// bitmap containers. Runs are not compressed. Upon request, the storage of the
// Roaring bitmap can be optimized using the runOptimize function. This triggers a
// scan through the array and bitmap containers that converts them, if helpful, to
// run containers. In a given application, this might be done prior to storing the
// bitmaps as immutable objects to be queried. Run containers may also arise from
// calling a function to add a range of values."
// Ref: https://arxiv.org/pdf/1603.06549 (Page 6)
optimize :: proc(rb: ^Roaring_Bitmap) -> (err: runtime.Allocator_Error) {
	containers := &rb.containers
	for key, container in containers {
		containers[key] = container_convert_to_optimal(container, rb.allocator) or_return
	}

	assert(len(rb.cindex) == len(rb.containers), "Containers and CIndex are out of sync!")
	return nil
}

// A Roaring_Bitmap is considered optimized if it has at least once Run_Container.
// This means optimize has been called on it at some point.
is_optimized :: proc(rb: Roaring_Bitmap) -> bool {
	for _, container in rb.containers {
		switch c in container {
		case Run_Container:
			return true
		case Array_Container, Bitmap_Container:
			continue
		}
	}

	return false
}

// Returns a u16 [little-endian] made up of the 16 most significant
// bits in a u32 number.
@(private, require_results)
most_significant :: proc(n: u32) -> u16 {
	as_bytes := transmute([4]byte)n
	return slice.to_type(as_bytes[2:4], u16)
}

// Returns a u16 [little-endian] made up of the 16 least significant
// bits in a u32 number.
@(private, require_results)
least_significant :: proc(n: u32) -> u16 {
	as_bytes := transmute([4]byte)n
	return slice.to_type(as_bytes[0:2], u16)
}

_main :: proc() {
	rb1, _ := init(context.temp_allocator)
	add_many(&rb1, 0, 1, 5, 6)

	rb2, _ := init(context.temp_allocator)
	add_many(&rb2, 0, 1, 2, 3, 4, 5)

	xor_inplace(&rb1, rb2)
	exp := [4]u32{2, 3, 4, 6}
	arr := to_array(rb1, context.temp_allocator)
	fmt.println(exp)
	fmt.println(arr)
	fmt.println(slice.equal(exp[:], arr[:]))
}

main :: proc() {
	when ODIN_DEBUG {
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		context.allocator = mem.tracking_allocator(&track)

		defer {
			if len(track.allocation_map) > 0 {
				fmt.eprintf("=== %v allocations not freed: ===\n", len(track.allocation_map))
				for _, entry in track.allocation_map {
					fmt.eprintf("- %v bytes @ %v\n", entry.size, entry.location)
				}
			}
			if len(track.bad_free_array) > 0 {
				fmt.eprintf("=== %v incorrect frees: ===\n", len(track.bad_free_array))
				for entry in track.bad_free_array {
					fmt.eprintf("- %p @ %v\n", entry.memory, entry.location)
				}
			}
			mem.tracking_allocator_destroy(&track)
		}
	}

	_main()
}
