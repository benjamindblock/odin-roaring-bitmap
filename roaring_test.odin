package roaring

import "core:fmt"
import "core:testing"

@(test)
test_setting_values_works_for_sparse :: proc(t: ^testing.T) {
	rb := make(Roaring_Bitmap)
	defer roaring_free(rb)

	roaring_set(&rb, 0)
	roaring_set(&rb, 1)
	roaring_set(&rb, 2)

	testing.expect_value(t, roaring_is_set(rb, 0), true)
	testing.expect_value(t, roaring_is_set(rb, 1), true)
	testing.expect_value(t, roaring_is_set(rb, 2), true)
	testing.expect_value(t, roaring_is_set(rb, 3), false)

	key: u16be
	count := 0
	container: Container
	for k, v in rb {
		key = k
		count += 1
		container = v
	}

	testing.expect_value(t, count, 1)
	sc, ok := container.(Sparse_Container)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, sc.cardinality, 3)

	// Unset the value 2 from the bitmap, ensure that it decreases
	// the cardinality.
	roaring_unset(&rb, 2)
	for k, v in rb {
		container = v
	}
	sc, ok = container.(Sparse_Container)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, sc.cardinality, 2)
	testing.expect_value(t, roaring_is_set(rb, 0), true)
	testing.expect_value(t, roaring_is_set(rb, 1), true)
	testing.expect_value(t, roaring_is_set(rb, 2), false)
	testing.expect_value(t, roaring_is_set(rb, 3), false)
}

@(test)
test_setting_values_works_for_dense :: proc(t: ^testing.T) {
	// Create a Roaring_Bitmap and assert that setting up to
	// 4096 values will use a Sparse_Container.
	rb := make(Roaring_Bitmap)
	defer roaring_free(rb)

	for i in 0..<4096 {
		roaring_set(&rb, u32be(i))
	}
	testing.expect_value(t, roaring_is_set(rb, 0), true)
	testing.expect_value(t, roaring_is_set(rb, 4095), true)

	count := 0
	container: Container
	for k, v in rb {
		count += 1
		container = v
	}
	testing.expect_value(t, count, 1)
	sc, sc_ok := container.(Sparse_Container)
	testing.expect_value(t, sc_ok, true)
	testing.expect_value(t, sc.cardinality, 4096)

	// Assert that setting the 4067th value will convert the Sparse_Container
	// into a Dense_Container.
	roaring_set(&rb, 4096)
	testing.expect_value(t, roaring_is_set(rb, 4096), true)

	count = 0
	for k, v in rb {
		count += 1
		container = v
	}
	testing.expect_value(t, count, 1)
	dc, dc_ok := container.(Dense_Container)
	testing.expect_value(t, dc_ok, true)
	testing.expect_value(t, dc.cardinality, 4097)

	// Assert that removing the 4097th value will convert the Dense_Container
	// back down to a Sparse_Container.
	roaring_unset(&rb, 4096)
	testing.expect_value(t, roaring_is_set(rb, 4096), false)
	for k, v in rb {
		container = v
	}
	sc, sc_ok = container.(Sparse_Container)
	testing.expect_value(t, sc_ok, true)
	testing.expect_value(t, sc.cardinality, 4096)
}

@(test)
test_multiple_sparse_containers :: proc(t: ^testing.T) {
	rb := make(Roaring_Bitmap)
	defer roaring_free(rb)

	roaring_set(&rb, 0)
	roaring_set(&rb, 1)
	roaring_set(&rb, 123456789)

	testing.expect_value(t, len(rb), 2)

	sc1, ok1 := rb[most_significant(0)].(Sparse_Container)
	testing.expect_value(t, ok1, true)
	testing.expect_value(t, sc1.cardinality, 2)

	sc2, ok2 := rb[most_significant(123456789)].(Sparse_Container)
	testing.expect_value(t, ok2, true)
	testing.expect_value(t, sc2.cardinality, 1)
}

@(test)
test_intersection_sparse :: proc(t: ^testing.T) {
	rb1 := make(Roaring_Bitmap)
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := make(Roaring_Bitmap)
	roaring_set(&rb2, 1)

	rb3 := roaring_intersection(rb1, rb2)
	testing.expect_value(t, roaring_is_set(rb3, 0), false)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)

	roaring_free(rb1)
	roaring_free(rb2)
	roaring_free(rb3)
}

@(test)
test_intersection_sparse_and_dense :: proc(t: ^testing.T) {
	rb1 := make(Roaring_Bitmap)
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := make(Roaring_Bitmap)
	for i in 0..=4096 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_intersection(rb1, rb2)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)
	testing.expect_value(t, roaring_is_set(rb3, 2), false)
	testing.expect_value(t, roaring_is_set(rb3, 4096), false)

	roaring_free(rb1)
	roaring_free(rb2)
	roaring_free(rb3)
}

@(test)
test_intersection_dense :: proc(t: ^testing.T) {
	rb1 := make(Roaring_Bitmap)
	for i in 0..=4096 {
		roaring_set(&rb1, u32be(i))
	}

	rb2 := make(Roaring_Bitmap)
	for i in 4096..=9999 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_intersection(rb1, rb2)
	testing.expect_value(t, len(rb3), 1)
	testing.expect_value(t, roaring_is_set(rb3, 4095), false)
	testing.expect_value(t, roaring_is_set(rb3, 4096), true)
	testing.expect_value(t, roaring_is_set(rb3, 4097), false)

	roaring_free(rb1)
	roaring_free(rb2)
	roaring_free(rb3)
}

@(test)
test_union_sparse :: proc(t: ^testing.T) {
	rb1 := make(Roaring_Bitmap)
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := make(Roaring_Bitmap)
	roaring_set(&rb2, 1)

	rb3 := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3), 1)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)

	roaring_free(rb1)
	roaring_free(rb2)
	roaring_free(rb3)
}

@(test)
test_union_sparse_and_dense :: proc(t: ^testing.T) {
	rb1 := make(Roaring_Bitmap)
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := make(Roaring_Bitmap)
	for i in 0..=4096 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3), 1)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)
	testing.expect_value(t, roaring_is_set(rb3, 2), true)
	testing.expect_value(t, roaring_is_set(rb3, 4096), true)
	testing.expect_value(t, roaring_is_set(rb3, 4097), false)

	roaring_free(rb1)
	roaring_free(rb2)
	roaring_free(rb3)
}

@(test)
test_union_dense :: proc(t: ^testing.T) {
	rb1 := make(Roaring_Bitmap)
	for i in 0..=4096 {
		roaring_set(&rb1, u32be(i))
	}

	rb2 := make(Roaring_Bitmap)
	for i in 123456789..=123456800 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3), 2)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 4095), true)
	testing.expect_value(t, roaring_is_set(rb3, 4096), true)
	testing.expect_value(t, roaring_is_set(rb3, 4097), false)
	testing.expect_value(t, roaring_is_set(rb3, 123456788), false)
	testing.expect_value(t, roaring_is_set(rb3, 123456789), true)
	testing.expect_value(t, roaring_is_set(rb3, 123456800), true)
	testing.expect_value(t, roaring_is_set(rb3, 123456801), false)

	roaring_free(rb1)
	roaring_free(rb2)
	roaring_free(rb3)
}

@(test)
test_bit_count :: proc(t: ^testing.T) {
	testing.expect_value(t, bit_count(0), 0)
	testing.expect_value(t, bit_count(1), 1)
	testing.expect_value(t, bit_count(2), 1)
	testing.expect_value(t, bit_count(3), 2)
}
