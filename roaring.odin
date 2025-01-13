package roaring

import "core:fmt"
import "core:mem"
import "core:slice"

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

Sparse_Container :: struct {
	packed_array: [dynamic]u16be,
	cardinality: int,
}

Dense_Container :: struct {
	bitmap: [dynamic]u8,
	cardinality: int,
}

Container :: union {
	Sparse_Container,
	Dense_Container,
}

Container_Index :: distinct map[u16be]Container

Roaring_Bitmap :: struct {
	index: Container_Index,
	allocator: mem.Allocator,
}

// Counts the number of set bits in an integer.
// FIXME: Can be optimized:
// http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetNaive
// Maybe just use a 256 element lookup table for a u8 that gives exactly
// the number of bits set for every possible byte.
bit_count :: proc(n: int) -> (c: int) {
	n := n
	for n != 0 {
		c += n & 1
		n >>= 1
	}
	return c
}

roaring_init :: proc(allocator := context.allocator) -> Roaring_Bitmap {
	index := make(Container_Index)
	return Roaring_Bitmap{index=index, allocator=allocator}
}

roaring_free :: proc(rb: ^Roaring_Bitmap) {
	for k, _ in rb.index {
		roaring_free_at(rb, k)
	}
	delete(rb.index)
}

roaring_free_at :: proc(rb: ^Roaring_Bitmap, i: u16be) {
	container := rb.index[i]
	switch c in container {
	case Sparse_Container:
		sparse_container_free(c)
	case Dense_Container:
		dense_container_free(c)
	}
	delete_key(&rb.index, i)
}

sparse_container_init :: proc(allocator := context.allocator) -> Sparse_Container {
	arr := make([dynamic]u16be)
	sc := Sparse_Container{
		packed_array=arr,
		cardinality=0,
	}
	return sc
}

sparse_container_free :: proc(sc: Sparse_Container) {
	delete(sc.packed_array)
}

dense_container_init :: proc(allocator := context.allocator) -> Dense_Container {
	// 2^16 / 8
	arr := make([dynamic]u8, 8192)
	dc := Dense_Container{
		bitmap=arr,
		cardinality=0,
	}
	return dc
}

dense_container_free :: proc(dc: Dense_Container) {
	delete(dc.bitmap)
}

// TODO: Add an error to be returned here if the value cannot be
// found in the bitmap.
//
// If a container doesn’t already exist then create a new array container,
// add it to the Roaring bitmap’s first-level index, and add N to the array.
roaring_set :: proc(
	rb: ^Roaring_Bitmap,
	n: u32be,
) -> (ok: bool, err: Roaring_Error) {
	i := most_significant(n)
	j := least_significant(n)

	// If there is no container to put the value in, create a new Sparse_Container
	// first and insert the value into it, and add to the roaring bitmap.
	if !(i in rb.index) {
		sc := sparse_container_init(rb.allocator)
		set_packed_array(&sc, j) or_return
		rb.index[i] = sc
		return true, nil
	}

	// If the container does exist, add it to the correct one.
	container := rb.index[i]
	switch &c in container {
	case Sparse_Container:
		// If an array container has 4,096 integers, first convert it to a
		// bitmap container. Then set the bit at N % 2^16.
		if c.cardinality == 4096 {
			dc := convert_container_from_sparse_to_dense(c)
			set_bitmap(&dc, j) or_return
			rb.index[i] = dc
		} else {
			set_packed_array(&c, j) or_return
			rb.index[i] = c
		}
	case Dense_Container:
		set_bitmap(&c, j) or_return
		rb.index[i] = c
	}

	return true, nil
}

roaring_unset :: proc(
	rb: ^Roaring_Bitmap,
	n: u32be,
) -> (ok: bool, err: Roaring_Error) {
	i := most_significant(n)
	j := least_significant(n)

	// If the container for this value does not exist, return
	// an error.
	if !(i in rb.index) {
		return false, Not_Set_Error{j}
	}

	container := rb.index[i]
	switch &c in container {
	case Sparse_Container:
		unset_packed_array(&c, j) or_return
		rb.index[i] = c
	case Dense_Container:
		unset_bitmap(&c, j) or_return

		// If we have returned to <= 4096 elements after unsetting a value, then convert
		// the dense bitmap back into the packed array (eg. sparse) representation.
		if c.cardinality == 4096 {
			sc := convert_container_from_dense_to_sparse(c, rb.allocator)
			rb.index[i] = sc
		} else {
			rb.index[i] = c
		}
	}

	// If we have removed the last element in a container, remove that key entirely.
	container = rb.index[i]
	switch c in container {
	case Sparse_Container:
		if c.cardinality == 0 {
			roaring_free_at(rb, i)
		}
	// NOTE: This case should never occur in regular usage because we convert dense
	// containers to sparse containers when they fall down to <4096 elements.
	case Dense_Container:
		if c.cardinality == 0 {
			roaring_free_at(rb, i)
		}
	}

	return true, nil
}

// To check if an integer N exists, get N’s 16 most significant bits (N / 2^16)
// and use it to find N’s corresponding container in the Roaring bitmap.
// If the container doesn’t exist, then N is not in the Roaring bitmap.
// Checking for existence in array and bitmap containers works differently:
//   Bitmap: check if the bit at N % 2^16 is set.
//   Array: use binary search to find N % 2^16 in the sorted array.
roaring_is_set :: proc(rb: Roaring_Bitmap, n: u32be) -> (found: bool) {
	i := most_significant(n)
	if !(i in rb.index) {
		return false
	}

	container := rb.index[i]
	j := least_significant(n)
	switch c in container {
	case Sparse_Container:
		found = is_set_packed_array(c, j)
	case Dense_Container:
		found = is_set_bitmap(c, j)
	}

	return found
}

// Returns a u16 in big-endian made up of the 16 most significant
// bits in a u32be number.
most_significant :: proc(n: u32be) -> u16be {
	as_bytes := transmute([4]byte)n
	return slice.to_type(as_bytes[0:2], u16be)
}

// Returns a u16 in big-endian made up of the 16 least significant
// bits in a u32be number.
least_significant :: proc(n: u32be) -> u16be {
	as_bytes := transmute([4]byte)n
	return slice.to_type(as_bytes[2:4], u16be)
}

set_packed_array :: proc(
	sc: ^Sparse_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	i, found := slice.binary_search(sc.packed_array[:], n)

	if found {
		return false, Already_Set_Error{n}
	}

	inject_at(&sc.packed_array, i, n)
	sc.cardinality += 1

	return true, nil
}

unset_packed_array :: proc(
	sc: ^Sparse_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	i, found := slice.binary_search(sc.packed_array[:], n)

	if !found {
		return false, Not_Set_Error{n}
	}

	ordered_remove(&sc.packed_array, i)
	sc.cardinality -= 1

	return true, nil
}

is_set_packed_array :: proc(sc: Sparse_Container, n: u16be) -> (found: bool) {
	_, found = slice.binary_search(sc.packed_array[:], n)		
	return found
}

set_bitmap :: proc(
	dc: ^Dense_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	if is_set_bitmap(dc^, n) {
		return false, Already_Set_Error{n}
	}

	bitmap := dc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)
	mask := u8(1 << bit_i)
	byte := bitmap[byte_i]
	bitmap[byte_i] = byte | mask

	dc.bitmap = bitmap
	dc.cardinality += 1

	return true, nil
}

unset_bitmap :: proc(
	dc: ^Dense_Container,
	n: u16be,
) -> (ok: bool, err: Roaring_Error) {
	if !is_set_bitmap(dc^, n) {
		return false, Not_Set_Error{n}
	}

	bitmap := dc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)
	mask := u8(1 << bit_i)

	byte := bitmap[byte_i]
	bitmap[byte_i] = byte & ~mask

	dc.bitmap = bitmap
	dc.cardinality -= 1

	return true, nil
}

// TODO: Add some assertions.
is_set_bitmap :: proc(dc: Dense_Container, n: u16be) -> (found: bool) {
	bitmap := dc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)

	// How to check if a specific bit is set:
	//   1. Store as 'temp': left shift 1 by k to create a number that has only the k-th bit set.
	//   2. If bitwise AND of n and 'temp' is non-zero, then the bit is set.
	byte := bitmap[byte_i]
	found = (byte & (1 << bit_i)) != 0

	return found
}

convert_container_from_sparse_to_dense :: proc(
	sc: Sparse_Container,
	allocator := context.allocator,
) -> Dense_Container {
	dc := dense_container_init(allocator)

	for i in sc.packed_array {
		set_bitmap(&dc, i)
	}

	sparse_container_free(sc)
	return dc
}

convert_container_from_dense_to_sparse :: proc(
	dc: Dense_Container,
	allocator := context.allocator,
) -> Sparse_Container {
	sc := sparse_container_init(allocator)

	for byte, i in dc.bitmap {
		for j in 0..<8 {
			bit_is_set := (byte & (1 << u8(j))) != 0
			if bit_is_set {
				total_i := u16be((i * 8) + j)
				set_packed_array(&sc, total_i)
			}
		}
	}

	dense_container_free(dc)
	return sc
}

clone_container :: proc(
	container: Container,
	allocator := context.allocator,
) -> Container {
	cloned: Container

	switch c in container {
	case Sparse_Container:
		new_packed_array := slice.clone_to_dynamic(c.packed_array[:], allocator)
		cloned = Sparse_Container{
			packed_array=new_packed_array,
			cardinality=c.cardinality,
		}
	case Dense_Container:
		new_bitmap := slice.clone_to_dynamic(c.bitmap[:], allocator)
		cloned = Dense_Container{
			bitmap=new_bitmap,
			cardinality=c.cardinality,
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
			case Sparse_Container:
				switch c2 in v2 {
				case Sparse_Container:
					rb.index[k1] = intersection_array_with_array(c1, c2, allocator)
				case Dense_Container:
					rb.index[k1] = intersection_array_with_bitmap(c1, c2, allocator)
				}
			case Dense_Container:
				switch c2 in v2 {
				case Sparse_Container:
					rb.index[k1] = intersection_array_with_bitmap(c2, c1, allocator)
				case Dense_Container:
					rb.index[k1] = intersection_bitmap_with_bitmap(c1, c2, allocator)
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
			case Sparse_Container:
				switch c2 in v2 {
				case Sparse_Container:
					rb.index[k1] = union_array_with_array(c1, c2, allocator)
				case Dense_Container:
					rb.index[k1] = union_array_with_bitmap(c1, c2, allocator)
				}
			case Dense_Container:
				switch c2 in v2 {
				case Sparse_Container:
					rb.index[k1] = union_array_with_bitmap(c2, c1, allocator)
				case Dense_Container:
					rb.index[k1] = union_bitmap_with_bitmap(c1, c2, allocator)
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
	sc1: Sparse_Container,
	sc2: Sparse_Container,
	allocator := context.allocator,
) -> Sparse_Container {
	new_cardinality: int
	if sc1.cardinality < sc2.cardinality {
		new_cardinality = sc1.cardinality
	} else {
		new_cardinality = sc2.cardinality
	}
	sc := sparse_container_init(allocator)

	// FIXME: Actually do both of these.

	// Used to check if the cardinalities differ by less than a factor of 64.
	// For intersections, we use a simple merge (akin to what is done in merge
	// sort) when the two arrays have cardinalities that differ by less than a
	// factor of 64. Otherwise, we use galloping intersections.
	// b1 := (sc1.cardinality / 64) < sc2.cardinality
	// b2 := sc2.cardinality < (sc1.cardinality * 64)
	// // Use the simple merge.
	// if b1 && b2 {
	// // Use a galloping intersection.
	// } else {
	// }

	// Naive implementation to start. Just binary search every value in c1 in the
	// c2 array. If found, add that value to the new array.
	for v in sc1.packed_array {
		if is_set_packed_array(sc2, v) {
			set_packed_array(&sc, v)
		}
	}

	return sc
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
	sc1: Sparse_Container,
	sc2: Sparse_Container,
	allocator := context.allocator,
) -> Container {
	if (sc1.cardinality + sc2.cardinality) <= 4096 {
		sc := sparse_container_init(allocator)
		for v in sc1.packed_array {
			set_packed_array(&sc, v)
		}
		// Only add the values from the second array *if* it has not already been added.
		for v in sc2.packed_array {
			if !is_set_packed_array(sc, v) {
				set_packed_array(&sc, v)
			}
		}
		return sc
	} else {
		dc := dense_container_init(allocator)
		for v in sc1.packed_array {
			set_bitmap(&dc, v)
		}
		for v in sc2.packed_array {
			if !is_set_bitmap(dc, v) {
				set_bitmap(&dc, v)
			}
		}
		return dc
	}
}

// The intersection between an array and a bitmap container can be computed
// quickly: we iterate over the values in the array container, checking the
// presence of each 16-bit integer in the bitmap container and generating a new
// array container that has as much capacity as the input array container.
intersection_array_with_bitmap :: proc(
	sc: Sparse_Container,
	dc: Dense_Container,
	allocator := context.allocator,
) -> Sparse_Container {
	new_sc := sparse_container_init(allocator)
	for v in sc.packed_array {
		if is_set_bitmap(dc, v) {
			set_packed_array(&new_sc, v)
		}
	}
	return new_sc
}

// Unions are also efficient: we create a copy of the bitmap and iterate over the
// array, setting the corresponding bits.
union_array_with_bitmap :: proc(
	sc: Sparse_Container,
	dc: Dense_Container,
	allocator := context.allocator,
) -> Dense_Container {
	new_container := clone_container(dc, allocator)
	new_dc := new_container.(Dense_Container)

	for v in sc.packed_array {
		if !is_set_bitmap(new_dc, v) {
			set_bitmap(&new_dc, v)
		}
	}

	return new_dc
}

// Bitmap vs Bitmap: To compute the intersection between two bitmaps, we first
// compute the cardinality of the result using the bitCount function over the
// bitwise AND of the corresponding pairs of words. If the intersection exceeds
// 4096, we materialize a bitmap container by recomputing the bitwise AND between
// the words and storing them in a new bitmap container. Otherwise, we generate a
// new array container by, once again, recomputing the bitwise ANDs, and iterating
// over their 1-bits.
intersection_bitmap_with_bitmap :: proc(
	dc1: Dense_Container,
	dc2: Dense_Container,
	allocator := context.allocator,
) -> Container {
	count := 0
	for byte1, i in dc1.bitmap {
		byte2 := dc2.bitmap[i]
		res := byte1 & byte2
		count += bit_count(int(res))
	}

	if count > 4096 {
		dc := dense_container_init(allocator)
		for byte1, i in dc1.bitmap {
			byte2 := dc2.bitmap[i]
			res := byte1 & byte2
			dc.bitmap[i] = res
			dc.cardinality += bit_count(int(res))
		}
		return dc
	} else {
		sc := sparse_container_init(allocator)
		for byte1, i in dc1.bitmap {
			byte2 := dc2.bitmap[i]
			res := byte1 & byte2
			for j in 0..<8 {
				bit_is_set := (res & (1 << u8(j))) != 0
				if bit_is_set {
					total_i := u16be((i * 8) + j)
					set_packed_array(&sc, total_i)
				}
			}
		}
		return sc
	}
}

// A union between two bitmap containers is straightforward: we execute the
// bitwise OR between all pairs of corresponding words. There are 1024 words in
// each container, so 1024 bitwise OR operations are needed. At the same time, we
// compute the cardinality of the result using the bitCount function on the
// generated words.
union_bitmap_with_bitmap :: proc(
	dc1: Dense_Container,
	dc2: Dense_Container,
	allocator := context.allocator,
) -> Dense_Container {
	dc := dense_container_init(allocator)
	for byte, i in dc1.bitmap {
		res := byte | dc2.bitmap[i]
		dc.bitmap[i] = res
		dc.cardinality = bit_count(int(res))
	}
	return dc
}

main :: proc() {
	fmt.println("Hello, world!")

	rb := roaring_init()
	defer roaring_free(&rb)

	// ok: bool
	// err: Roaring_Error

	ok, err := roaring_set(&rb, 0)
	fmt.println("ok", ok, "err", err)

	ok, err = roaring_unset(&rb, 0)
	fmt.println("ok", ok, "err", err)

	ok, err = roaring_unset(&rb, 0)
	fmt.println("ok", ok, "err", err)

	// roaring_set(x, &rb)
	// fmt.println("roaring bitmap AFTER INSERT:", rb)
	// fmt.println("FOUND", x, roaring_is_set(x, rb))

	// y: u32be = 2
	// roaring_set(y, &rb)
	// fmt.println("roaring bitmap AFTER INSERT:", rb)
	// fmt.println("FOUND", y, roaring_is_set(y, rb))

	// fmt.printf("{:16b}\n", 1 << 16)
	// fmt.printf("{:16b}\n", 1 << 1)

	// roaring_unset(y, &rb)
	// fmt.println("roaring bitmap AFTER REMOVE:", rb)
	// roaring_unset(x, &rb)
	// fmt.println("roaring bitmap AFTER REMOVE:", rb)


	// // roaring_set(12345678, &index)
	// fmt.println(index)
	// roaring_unset(u32be(0), &index)
	// fmt.println(index)

	// roaring_unset(u32be(1), &index)
	// fmt.println(index)

	// roaring_set(u32be(1), &index)
	// roaring_set(u32be(0), &index)
	// fmt.println(index)

	// dc := dense_container_init()
	// set_bitmap(&dc, 15)
	// fmt.println(is_set_bitmap(dc, 15))
	// set_bitmap(&dc, 16)
	// fmt.println(is_set_bitmap(dc, 16))

	// // Convert the u32be into a byte slice
	// b := transmute([4]byte)x
	// fmt.println(b)
	// fmt.println(slice.to_type(b[:], u32be))

	// // Most significant 16 bits
	// fmt.println(b[0:2])
	// fmt.println(slice.to_type(b[0:2], u16be))
	// fmt.printf("{:16b}\n", slice.to_type(b[0:2], u16be))

	// // Least significant 16 bits
	// fmt.println(b[2:4])
	// fmt.println(slice.to_type(b[2:4], u16be))
	// fmt.printf("{:16b}\n", slice.to_type(b[2:4], u16be))
}
