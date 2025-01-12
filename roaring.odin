package roaring

import "core:fmt"
import "core:slice"
// import "core:mem"

MOST_SIG: u32 = 0b00000000000000001111111111111111
LEAST_SIG: u32 = 0b11111111111111110000000000000000

// Index of the maximum chunk.
// Also the size of each chunk.

// Chunk :: struct {
// 	start: u16,
// 	end: u16,
// }

// NOTE: The packed_array must be sorted.
Sparse_Container :: struct {
	packed_array: [dynamic]u16be,
	cardinality: int,
}

Dense_Container :: struct {
	// 2^16 / 8
	bitmap: [8192]u8,
	cardinality: int,
}

Container :: union {
	Sparse_Container,
	Dense_Container,
}

Index :: distinct map[u16be]Container

sparse_container_init :: proc() -> Sparse_Container {
	arr := make([dynamic]u16be)
	sc := Sparse_Container{
		packed_array=arr,
		cardinality=0,
	}
	return sc
}

dense_container_init :: proc() -> Dense_Container {
	arr := new([8192]u8)
	dc := Dense_Container{
		bitmap=arr^,
		cardinality=0,
	}
	return dc
}

// If a container doesn’t already exist then create a new array container,
// add it to the Roaring bitmap’s first-level index, and add N to the array.
insert_to_roaring :: proc(n: u32be, index: ^Index) {
	// If the value is already in the bitmap, do nothing.
	if is_present(n, index^) {
		return
	}

	i := most_significant(n)
	j := least_significant(n)

	// If there is no container to put the value in, create a new Sparse_Container
	// first and insert the value into it, and add to the index.
	if !(i in index) {
		sc := sparse_container_init()
		append(&sc.packed_array, j)
		sc.cardinality += 1
		index[i] = sc
		return
	}

	// If the container does exist, add it to the correct one.
	container := index[i]
	switch &c in container {
	case Sparse_Container:
		if c.cardinality == 4096 {
			fmt.println("CONVERTING")
			dc := convert_from_sparse_to_dense(c)
			index[i] = dc
			insert_to_roaring(n, index)
			return
		}

		// FIXME: Use find+inject instead? Is that faster?
		append(&c.packed_array, j)
		slice.sort(c.packed_array[:])
		c.cardinality += 1
		index[i] = c
	case Dense_Container:
		set_bitmap(&c, j)
		index[i] = c
	}
}

// To check if an integer N exists, get N’s 16 most significant bits (N / 2^16)
// and use it to find N’s corresponding container in the Roaring bitmap.
// If the container doesn’t exist, then N is not in the Roaring bitmap.
// Checking for existence in array and bitmap containers works differently:
//   Bitmap: check if the bit at N % 2^16 is set.
//   Array: use binary search to find N % 2^16 in the sorted array.
is_present :: proc(n: u32be, index: Index) -> (found: bool) {
	i := most_significant(n)
	if !(i in index) {
		return false
	}

	container := index[i]
	switch c in container {
	case Sparse_Container:
		_, found = slice.binary_search(c.packed_array[:], least_significant(n))		
	case Dense_Container:
		found = is_set_bitmap(c, n)
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

// TODO: Add some assertions.
set_bitmap :: proc(dc: ^Dense_Container, n: u16be) {
	bitmap := dc.bitmap

	byte_i := n / 8
	bit_i := n - (byte_i * 8)
	mask := u8(1 << bit_i)
	byte := bitmap[byte_i]
	bitmap[byte_i] = byte | mask

	dc.bitmap = bitmap
	dc.cardinality += 1
}

// TODO: Make sure to deallocate the Sparse_Container here.
convert_from_sparse_to_dense :: proc(sc: Sparse_Container) -> Dense_Container {
	dc := dense_container_init()

	for i in sc.packed_array {
		set_bitmap(&dc, i)
	}

	return dc
}

// TODO: Add some assertions.
is_set_bitmap :: proc(dc: Dense_Container, n: u32be) -> (found: bool) {
	loc := least_significant(n)
	bitmap := dc.bitmap

	byte_i := loc / 8
	byte := bitmap[byte_i]

	bit_i := loc - (byte_i * 8)

	// How to check if a specific bit is set:
	//   1. Store as 'temp': left shift 1 by k to create a number that has only the k-th bit set.
	//   2. If bitwise AND of n and 'temp' is non-zero, then the bit is set.
	found = (byte & (1 << bit_i)) != 0

	return found
}

// check_bitmap :: proc(dc: ^Dense_Container) -> bool {
// 	return false
// }

main :: proc() {
	fmt.println("Hello, world!")
	x: u32be = 4
	fmt.println(x)
	fmt.printf("{:32b}\n", x)

	index := make(Index)
	fmt.println(index)

	insert_to_roaring(x, &index)
	fmt.println("INDEX AFTER INSERT:", index)
	fmt.println("FOUND", x, is_present(x, index))

	y: u32be = 2
	insert_to_roaring(y, &index)
	fmt.println("INDEX AFTER INSERT:", index)
	fmt.println("FOUND", y, is_present(y, index))

	fmt.printf("{:16b}\n", 1 << 16)
	fmt.printf("{:16b}\n", 1 << 1)

	for i in 0..=4096 {
		insert_to_roaring(u32be(i), &index)
	}

	insert_to_roaring(12345678, &index)
	fmt.println(index)


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
