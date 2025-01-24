package roaring

import "base:runtime"
import "core:slice"

@(private, require_results)
array_container_init :: proc(
	allocator := context.allocator
) -> (Array_Container, runtime.Allocator_Error) {
	arr, err := make([dynamic]u16be, allocator)
	ac := Array_Container{
		packed_array=arr,
		cardinality=0,
	}
	return ac, err
}

@(private)
array_container_free :: proc(ac: Array_Container) {
	delete(ac.packed_array)
}

@(private)
array_container_add :: proc(
	ac: ^Array_Container,
	n: u16be,
) -> (ok: bool, err: runtime.Allocator_Error) {
	i, found := slice.binary_search(ac.packed_array[:], n)

	if !found {
		inject_at(&ac.packed_array, i, n) or_return
		ac.cardinality += 1
		return true, nil
	} else {
		return false, nil
	}
}

@(private)
array_container_remove :: proc(
	ac: ^Array_Container,
	n: u16be,
) -> (ok: bool, err: runtime.Allocator_Error) {
	i, found := slice.binary_search(ac.packed_array[:], n)

	if found {
		ordered_remove(&ac.packed_array, i)
		ac.cardinality -= 1
		return true, nil
	} else {
		return false, nil
	}
}

@(private)
array_container_contains :: proc(ac: Array_Container, n: u16be) -> (found: bool) {
	_, found = slice.binary_search(ac.packed_array[:], n)		
	return found
}

// Finds the cardinality of a Array_Container.
@(private)
array_container_calculate_cardinality :: proc(ac: Array_Container) -> int {
	return len(ac.packed_array)
}

// The intersection between two array containers is always a new array container.
// We allocate a new array container that has its capacity set to the minimum of
// the cardinalities of the input arrays. When the two input array containers have
// similar cardinalities c1 and c2 (c1/64 < c2 < 64c1), we use a straightforward
// merge algorithm with algorithmic complexity O(c1 + c2), otherwise we use a
// galloping intersection with complexity O(min(c1, c2) log max(c1, c2)). We
// arrived at this threshold (c1/64 < c2 < 64c1) empirically as a reasonable
// choice, but it has not been finely tuned.
@(private)
array_container_and_array_container :: proc(
	ac1: Array_Container,
	ac2: Array_Container,
	allocator := context.allocator,
) -> (ac: Array_Container, err: runtime.Allocator_Error) {
	ac = array_container_init(allocator) or_return

	// Iterate over the smaller container and find all the values that match
	// from the larger. This helps to reduce the no. of binary searches we
	// need to perform.
	if ac1.cardinality < ac2.cardinality {
		for v in ac1.packed_array {
			if array_container_contains(ac2, v) {
				array_container_add(&ac, v) or_return
			}
		}
	} else {
		for v in ac2.packed_array {
			if array_container_contains(ac1, v) {
				array_container_add(&ac, v) or_return
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

	return ac, nil
}

// The intersection between an array and a bitmap container can be computed
// quickly: we iterate over the values in the array container, checking the
// presence of each 16-bit integer in the bitmap container and generating a new
// array container that has as much capacity as the input array container.
@(private)
array_container_and_bitmap_container :: proc(
	ac: Array_Container,
	bc: Bitmap_Container,
	allocator := context.allocator,
) -> (new_ac: Array_Container, err: runtime.Allocator_Error) {
	new_ac = array_container_init(allocator) or_return
	for v in ac.packed_array {
		if bitmap_container_contains(bc, v) {
			array_container_add(&new_ac, v) or_return
		}
	}
	return new_ac, nil
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
@(private)
array_container_and_run_container :: proc(
	ac: Array_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> (new_ac: Array_Container, err: runtime.Allocator_Error) {
	new_ac = array_container_init(allocator) or_return

	if ac.cardinality == 0 || len(rc.run_list) == 0 {
		return new_ac, nil
	}

	i := 0
	array_loop: for array_val in ac.packed_array {
		run_loop: for {
			run := rc.run_list[i]
			// If the run contains this array value, set it in the new array containing
			// the intersection and continue at the outer loop with the next array value.
			if run_contains(run, int(array_val)) {
				array_container_add(&new_ac, array_val) or_return
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

	return new_ac, nil
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
@(private)
array_container_or_array_container :: proc(
	ac1: Array_Container,
	ac2: Array_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	if (ac1.cardinality + ac2.cardinality) <= MAX_ARRAY_LENGTH {
		ac := array_container_init(allocator) or_return
		for v in ac1.packed_array {
			array_container_add(&ac, v) or_return
		}
		// Only add the values from the second array *if* it has not already been added.
		for v in ac2.packed_array {
			if !array_container_contains(ac, v) {
				array_container_add(&ac, v) or_return
			}
		}
		c = ac
	} else {
		bc := bitmap_container_init(allocator) or_return
		for v in ac1.packed_array {
			bitmap_container_add(&bc, v) or_return
		}
		for v in ac2.packed_array {
			if !bitmap_container_contains(bc, v) {
				bitmap_container_add(&bc, v) or_return
			}
		}
		c = bc
	}

	return c, nil
}

// Unions are also efficient: we create a copy of the bitmap and iterate over the
// array, setting the corresponding bits.
@(private)
array_container_or_bitmap_container :: proc(
	ac: Array_Container,
	bc: Bitmap_Container,
	allocator := context.allocator,
) -> (new_bc: Bitmap_Container, err: runtime.Allocator_Error) {
	new_container := container_clone(bc, allocator) or_return
	new_bc = new_container.(Bitmap_Container)

	for v in ac.packed_array {
		if !bitmap_container_contains(new_bc, v) {
			bitmap_container_add(&new_bc, v) or_return
		}
	}

	return new_bc, nil
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
@(private)
array_container_or_run_container :: proc(
	ac: Array_Container,
	rc: Run_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	c = container_clone(rc, allocator) or_return
	new_rc := c.(Run_Container)

	for v in ac.packed_array {
		run_container_add(&new_rc, v) or_return
	}

	return container_convert_to_optimal(new_rc, allocator)
}

// Array_Container => Bitmap_Container
@(private)
array_container_convert_to_bitmap_container :: proc(
	ac: Array_Container,
	allocator := context.allocator,
) -> (bc: Bitmap_Container, err: runtime.Allocator_Error) {
	bc = bitmap_container_init(allocator) or_return

	for i in ac.packed_array {
		bitmap_container_add(&bc, i) or_return
	}

	array_container_free(ac)
	return bc, nil
}

