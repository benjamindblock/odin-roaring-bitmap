package roaring

import "core:testing"

@(test)
test_setting_values_works_for_sparse :: proc(t: ^testing.T) {
	rb := roaring_init()
	defer roaring_free(&rb)

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
	for k, v in rb.index {
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
	for _, v in rb.index {
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
	rb := roaring_init()
	defer roaring_free(&rb)

	for i in 0..<4096 {
		roaring_set(&rb, u32be(i))
	}
	testing.expect_value(t, roaring_is_set(rb, 0), true)
	testing.expect_value(t, roaring_is_set(rb, 4095), true)

	count := 0
	container: Container
	for _, v in rb.index {
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
	for _, v in rb.index {
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
	for _, v in rb.index {
		container = v
	}
	sc, sc_ok = container.(Sparse_Container)
	testing.expect_value(t, sc_ok, true)
	testing.expect_value(t, sc.cardinality, 4096)
}

@(test)
test_setting_values_for_run_container :: proc(t: ^testing.T) {
	rc := run_container_init()
	defer run_container_free(rc)

	set_run_list(&rc, 0)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, is_set_run_list(rc, 0), true)
	testing.expect_value(t, is_set_run_list(rc, 1), false)

	set_run_list(&rc, 1)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, is_set_run_list(rc, 0), true)
	testing.expect_value(t, is_set_run_list(rc, 1), true)
}

@(test)
test_multiple_sparse_containers :: proc(t: ^testing.T) {
	rb := roaring_init()
	defer roaring_free(&rb)

	roaring_set(&rb, 0)
	roaring_set(&rb, 1)
	roaring_set(&rb, 123456789)

	testing.expect_value(t, len(rb.index), 2)

	sc1, ok1 := rb.index[most_significant(0)].(Sparse_Container)
	testing.expect_value(t, ok1, true)
	testing.expect_value(t, sc1.cardinality, 2)

	sc2, ok2 := rb.index[most_significant(123456789)].(Sparse_Container)
	testing.expect_value(t, ok2, true)
	testing.expect_value(t, sc2.cardinality, 1)
}

@(test)
test_intersection_sparse :: proc(t: ^testing.T) {
	rb1 := roaring_init()
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := roaring_init()
	roaring_set(&rb2, 1)

	rb3 := roaring_intersection(rb1, rb2)
	testing.expect_value(t, roaring_is_set(rb3, 0), false)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)

	roaring_free(&rb1)
	roaring_free(&rb2)
	roaring_free(&rb3)
}

@(test)
test_intersection_sparse_and_dense :: proc(t: ^testing.T) {
	rb1 := roaring_init()
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := roaring_init()
	for i in 0..=4096 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_intersection(rb1, rb2)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)
	testing.expect_value(t, roaring_is_set(rb3, 2), false)
	testing.expect_value(t, roaring_is_set(rb3, 4096), false)

	roaring_free(&rb1)
	roaring_free(&rb2)
	roaring_free(&rb3)
}

@(test)
test_intersection_dense :: proc(t: ^testing.T) {
	rb1 := roaring_init()
	for i in 0..=4096 {
		roaring_set(&rb1, u32be(i))
	}

	rb2 := roaring_init()
	for i in 4096..=9999 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_intersection(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 1)
	testing.expect_value(t, roaring_is_set(rb3, 4095), false)
	testing.expect_value(t, roaring_is_set(rb3, 4096), true)
	testing.expect_value(t, roaring_is_set(rb3, 4097), false)

	roaring_free(&rb1)
	roaring_free(&rb2)
	roaring_free(&rb3)
}

@(test)
test_union_sparse :: proc(t: ^testing.T) {
	rb1 := roaring_init()
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := roaring_init()
	roaring_set(&rb2, 1)

	rb3 := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 1)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)

	roaring_free(&rb1)
	roaring_free(&rb2)
	roaring_free(&rb3)
}

@(test)
test_union_sparse_and_dense :: proc(t: ^testing.T) {
	rb1 := roaring_init()
	roaring_set(&rb1, 0)
	roaring_set(&rb1, 1)

	rb2 := roaring_init()
	for i in 0..=4096 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 1)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 1), true)
	testing.expect_value(t, roaring_is_set(rb3, 2), true)
	testing.expect_value(t, roaring_is_set(rb3, 4096), true)
	testing.expect_value(t, roaring_is_set(rb3, 4097), false)

	roaring_free(&rb1)
	roaring_free(&rb2)
	roaring_free(&rb3)
}

@(test)
test_union_dense :: proc(t: ^testing.T) {
	rb1 := roaring_init()
	for i in 0..=4096 {
		roaring_set(&rb1, u32be(i))
	}

	rb2 := roaring_init()
	for i in 123456789..=123456800 {
		roaring_set(&rb2, u32be(i))
	}

	rb3 := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 2)
	testing.expect_value(t, roaring_is_set(rb3, 0), true)
	testing.expect_value(t, roaring_is_set(rb3, 4095), true)
	testing.expect_value(t, roaring_is_set(rb3, 4096), true)
	testing.expect_value(t, roaring_is_set(rb3, 4097), false)
	testing.expect_value(t, roaring_is_set(rb3, 123456788), false)
	testing.expect_value(t, roaring_is_set(rb3, 123456789), true)
	testing.expect_value(t, roaring_is_set(rb3, 123456800), true)
	testing.expect_value(t, roaring_is_set(rb3, 123456801), false)

	roaring_free(&rb1)
	roaring_free(&rb2)
	roaring_free(&rb3)
}

@(test)
test_bit_count :: proc(t: ^testing.T) {
	testing.expect_value(t, bit_count(0), 0)
	testing.expect_value(t, bit_count(1), 1)
	testing.expect_value(t, bit_count(2), 1)
	testing.expect_value(t, bit_count(3), 2)
}

@(test)
test_errors_thrown :: proc(t: ^testing.T) {
	rb := roaring_init()
	defer roaring_free(&rb)

	// Ensure we don't prefill the packed array with any 0 values
	// after initializing.
	testing.expect_value(t, roaring_is_set(rb, 0), false)

	ok: bool
	err: Roaring_Error

	// Assert we insert without errors.
	ok, err = roaring_set(&rb, 0)
	testing.expect_value(t, roaring_is_set(rb, 0), true)
	testing.expect_value(t, len(rb.index), 1)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, err, nil)

	// Attempting to insert again causes an Already_Set_Error to be returned.
	ok, err = roaring_set(&rb, 0)
	testing.expect_value(t, roaring_is_set(rb, 0), true)
	testing.expect_value(t, len(rb.index), 1)
	testing.expect_value(t, ok, false)
	_, ok = err.(Already_Set_Error)
	testing.expect_value(t, ok, true)

	// // Unsetting works as expected.
	ok, err = roaring_unset(&rb, 0)
	testing.expect_value(t, roaring_is_set(rb, 0), false)
	testing.expect_value(t, len(rb.index), 0)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, err, nil)

	// Unsetting the same value again causes an error.
	ok, err = roaring_unset(&rb, 0)
	testing.expect_value(t, roaring_is_set(rb, 0), false)
	testing.expect_value(t, len(rb.index), 0)
	testing.expect_value(t, ok, false)
	_, ok = err.(Not_Set_Error)
	testing.expect_value(t, ok, true)
}

@(test)
test_count_runs :: proc(t: ^testing.T) {
	rb := roaring_init()
	defer roaring_free(&rb)

	for i in 0..<10000 {
		if i % 2 == 0 {
			roaring_set(&rb, u32be(i))
		}
	}

	// Should have 5000 runs, each of length 1.
	dc := rb.index[0].(Dense_Container)
	runs := count_runs(dc)
	testing.expect_value(t, runs, 5000)
}

@(test)
test_should_convert_to_run_container :: proc(t: ^testing.T) {
	rb := roaring_init()
	defer roaring_free(&rb)

	roaring_set(&rb, 0)
	should := should_convert_to_run_container(rb.index[0])
	testing.expect_value(t, should, false)

	for i in 1..<10000 {
		if i % 2 == 0 {
			roaring_set(&rb, u32be(i))
		}
	}

	// Should have 5000 runs, each of length 1.
	should = should_convert_to_run_container(rb.index[0])
	testing.expect_value(t, should, true)
}

@(test)
test_least_significant_bit_i :: proc(t: ^testing.T) {
	n: u8 = 0b1101000
	testing.expect_value(t, 3, least_significant_bit_i(n))

	n = 0b00000000
	testing.expect_value(t, -1, least_significant_bit_i(n))

	n = 0b11111111
	testing.expect_value(t, 0, least_significant_bit_i(n))

	n = 0b10000000
	testing.expect_value(t, 7, least_significant_bit_i(n))
}

@(test)
test_least_significant_zero_bit_i :: proc(t: ^testing.T) {
	n: u8 = 0b10110111
	testing.expect_value(t, 3, least_significant_zero_bit_i(n))

	n = 0b10111111
	testing.expect_value(t, 6, least_significant_zero_bit_i(n))

	n = 0b00000000
	testing.expect_value(t, 0, least_significant_zero_bit_i(n))

	n = 0b11111111
	testing.expect_value(t, -1, least_significant_zero_bit_i(n))
}

@(test)
test_convert_bitmap_to_run_list :: proc(t: ^testing.T) {
	rb := roaring_init()
	defer roaring_free(&rb)

	roaring_set(&rb, 1)
	roaring_set(&rb, 2)

	roaring_set(&rb, 4)
	roaring_set(&rb, 5)
	roaring_set(&rb, 6)
	roaring_set(&rb, 7)
	roaring_set(&rb, 8)
	roaring_set(&rb, 9)

	for i in 12..<10000 {
		if i % 2 == 0 {
			roaring_set(&rb, u32be(i))
		}
	}

	run_list := convert_bitmap_to_run_list(rb.index[0].(Dense_Container))
	defer delete(run_list)
	exp_run: Run

	exp_run = Run{start=1, length=2}
	testing.expect_value(t, run_list[0], exp_run)

	exp_run = Run{start=4, length=6}
	testing.expect_value(t, run_list[1], exp_run)

	exp_run = Run{start=12, length=1}
	testing.expect_value(t, run_list[2], exp_run)
}
