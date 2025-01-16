package roaring

import "core:testing"

@(test)
test_flip_and_select :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	testing.expect_value(t, select(rb, 0), 0)
	testing.expect_value(t, select(rb, 2), 0)

	flip(&rb, 2)
	testing.expect_value(t, select(rb, 0), 0)
	testing.expect_value(t, select(rb, 2), 1)
}

@(test)
test_clone :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	add(&rb, 2)
	testing.expect_value(t, select(rb, 2), 1)

	rb2, _ := roaring_bitmap_clone(rb)
	defer roaring_bitmap_free(&rb2)
	testing.expect_value(t, select(rb2, 2), 1)
}

@(test)
test_setting_values_works_for_array :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	add(&rb, 0)
	add(&rb, 1)
	add(&rb, 2)

	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, contains(rb, 1), true)
	testing.expect_value(t, contains(rb, 2), true)
	testing.expect_value(t, contains(rb, 3), false)

	key: u16be
	count := 0
	container: Container
	for k, v in rb.index {
		key = k
		count += 1
		container = v
	}

	testing.expect_value(t, count, 1)
	ac, ok := container.(Array_Container)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, ac.cardinality, 3)

	// Unset the value 2 from the bitmap, ensure that it decreases
	// the cardinality.
	remove(&rb, 2)
	for _, v in rb.index {
		container = v
	}
	ac, ok = container.(Array_Container)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, ac.cardinality, 2)
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, contains(rb, 1), true)
	testing.expect_value(t, contains(rb, 2), false)
	testing.expect_value(t, contains(rb, 3), false)
}

@(test)
test_setting_values_works_for_bitmap :: proc(t: ^testing.T) {
	// Create a Roaring_Bitmap and assert that setting up to
	// 4096 values will use a Array_Container.
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	for i in 0..<4096 {
		add(&rb, i)
	}
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, contains(rb, 4095), true)

	count := 0
	container: Container
	for _, v in rb.index {
		count += 1
		container = v
	}
	testing.expect_value(t, count, 1)
	ac, ac_ok := container.(Array_Container)
	testing.expect_value(t, ac_ok, true)
	testing.expect_value(t, ac.cardinality, 4096)

	// Assert that setting the 4067th value will convert the Array_Container
	// into a Bitmap_Container.
	add(&rb, 4096)
	testing.expect_value(t, contains(rb, 4096), true)

	count = 0
	for _, v in rb.index {
		count += 1
		container = v
	}
	testing.expect_value(t, count, 1)
	bc, bc_ok := container.(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 4097)

	// Assert that removing the 4097th value will convert the Bitmap_Container
	// back down to a Array_Container.
	remove(&rb, 4096)
	testing.expect_value(t, contains(rb, 4096), false)
	for _, v in rb.index {
		container = v
	}
	ac, ac_ok = container.(Array_Container)
	testing.expect_value(t, ac_ok, true)
	testing.expect_value(t, ac.cardinality, 4096)
}

@(test)
test_setting_values_for_run_container :: proc(t: ^testing.T) {
	rc, _ := run_container_init()
	defer run_container_free(rc)

	run_container_add(&rc, 0)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, run_container_contains(rc, 0), true)
	testing.expect_value(t, run_container_contains(rc, 1), false)

	run_container_add(&rc, 1)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, run_container_contains(rc, 0), true)
	testing.expect_value(t, run_container_contains(rc, 1), true)
}

@(test)
test_setting_values_for_run_container_complex :: proc(t: ^testing.T) {
	rc, _ := run_container_init()
	defer run_container_free(rc)

	run_container_add(&rc, 3)
	run_container_add(&rc, 4)
	run_container_add(&rc, 0)
	run_container_add(&rc, 2)

	testing.expect_value(t, len(rc.run_list), 2)
	testing.expect_value(t, rc.run_list[0], Run{0, 1})
	testing.expect_value(t, rc.run_list[1], Run{2, 3})

	run_container_add(&rc, 5)
	testing.expect_value(t, len(rc.run_list), 2)
	testing.expect_value(t, rc.run_list[0], Run{0, 1})
	testing.expect_value(t, rc.run_list[1], Run{2, 4})

	run_container_add(&rc, 1)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, rc.run_list[0], Run{0, 6})

	run_container_remove(&rc, 1)
	testing.expect_value(t, rc.run_list[0], Run{0, 1})
	testing.expect_value(t, rc.run_list[1], Run{2, 4})
}

@(test)
test_converting_from_bitmap_to_run_container :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	// Confirm all 5000 bits are set in the Bitmap_Container.
	for i in 0..<5000 {
		add(&rb, i)
	}
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, contains(rb, 4999), true)
	container := rb.index[0]
	bc, bc_ok := container.(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 5000)
	testing.expect_value(t, should_convert_container_bitmap_to_run(bc), true)

	optimize(&rb)
	container = rb.index[0]
	rc, rc_ok := container.(Run_Container)
	testing.expect_value(t, rc_ok, true)
	testing.expect_value(t, run_container_calculate_cardinality(rc), 5000)

	exp_run := Run{start=0, length=5000}
	testing.expect_value(t, rc.run_list[0], exp_run)
}

@(test)
test_converting_from_run_to_bitmap_container :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	// Confirm all 6000 bits are set in the Bitmap_Container.
	for i in 0..<6000 {
		add(&rb, i)
	}
	optimize(&rb)

	container := rb.index[0]
	rc, rc_ok := container.(Run_Container)
	testing.expect_value(t, rc_ok, true)
	testing.expect_value(t, run_container_calculate_cardinality(rc), 6000)
	testing.expect_value(t, len(rc.run_list), 1)

	for i in 0..=4094 {
		if i % 2 == 0 {
			remove(&rb, i)
		}
	}

	container = rb.index[0]
	bc, bc_ok := container.(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 3952)
	testing.expect_value(t, bitmap_container_count_runs(bc), 2048)
}

@(test)
test_multiple_array_containers :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	add(&rb, 0)
	add(&rb, 1)
	add(&rb, 123456789)

	testing.expect_value(t, len(rb.index), 2)

	ac1, ok1 := rb.index[most_significant(0)].(Array_Container)
	testing.expect_value(t, ok1, true)
	testing.expect_value(t, ac1.cardinality, 2)

	ac2, ok2 := rb.index[most_significant(123456789)].(Array_Container)
	testing.expect_value(t, ok2, true)
	testing.expect_value(t, ac2.cardinality, 1)
}

@(test)
test_intersection_array :: proc(t: ^testing.T) {
	rb1, _ := roaring_bitmap_init()
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := roaring_bitmap_init()
	add(&rb2, 1)

	rb3, _ := roaring_intersection(rb1, rb2)
	testing.expect_value(t, contains(rb3, 0), false)
	testing.expect_value(t, contains(rb3, 1), true)

	roaring_bitmap_free(&rb1)
	roaring_bitmap_free(&rb2)
	roaring_bitmap_free(&rb3)
}

@(test)
test_intersection_array_and_bitmap :: proc(t: ^testing.T) {
	rb1, _ := roaring_bitmap_init()
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := roaring_bitmap_init()
	for i in 0..=4096 {
		add(&rb2, i)
	}

	rb3, _ := roaring_intersection(rb1, rb2)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 1), true)
	testing.expect_value(t, contains(rb3, 2), false)
	testing.expect_value(t, contains(rb3, 4096), false)

	roaring_bitmap_free(&rb1)
	roaring_bitmap_free(&rb2)
	roaring_bitmap_free(&rb3)
}

@(test)
test_intersection_bitmap :: proc(t: ^testing.T) {
	rb1, _ := roaring_bitmap_init()
	for i in 0..=4096 {
		add(&rb1, i)
	}

	rb2, _ := roaring_bitmap_init()
	for i in 4096..=9999 {
		add(&rb2, i)
	}

	rb3, _ := roaring_intersection(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 1)
	testing.expect_value(t, contains(rb3, 4095), false)
	testing.expect_value(t, contains(rb3, 4096), true)
	testing.expect_value(t, contains(rb3, 4097), false)

	roaring_bitmap_free(&rb1)
	roaring_bitmap_free(&rb2)
	roaring_bitmap_free(&rb3)
}

@(test)
test_union_array :: proc(t: ^testing.T) {
	rb1, _ := roaring_bitmap_init()
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := roaring_bitmap_init()
	add(&rb2, 1)

	rb3, _ := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 1)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 1), true)

	roaring_bitmap_free(&rb1)
	roaring_bitmap_free(&rb2)
	roaring_bitmap_free(&rb3)
}

@(test)
test_union_array_and_bitmap :: proc(t: ^testing.T) {
	rb1, _ := roaring_bitmap_init()
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := roaring_bitmap_init()
	for i in 0..=4096 {
		add(&rb2, i)
	}

	rb3, _ := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 1)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 1), true)
	testing.expect_value(t, contains(rb3, 2), true)
	testing.expect_value(t, contains(rb3, 4096), true)
	testing.expect_value(t, contains(rb3, 4097), false)

	roaring_bitmap_free(&rb1)
	roaring_bitmap_free(&rb2)
	roaring_bitmap_free(&rb3)
}

@(test)
test_union_bitmap :: proc(t: ^testing.T) {
	rb1, _ := roaring_bitmap_init()
	for i in 0..=4096 {
		add(&rb1, i)
	}

	rb2, _ := roaring_bitmap_init()
	for i in 123456789..=123456800 {
		add(&rb2, i)
	}

	rb3, _ := roaring_union(rb1, rb2)
	testing.expect_value(t, len(rb3.index), 2)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 4095), true)
	testing.expect_value(t, contains(rb3, 4096), true)
	testing.expect_value(t, contains(rb3, 4097), false)
	testing.expect_value(t, contains(rb3, 123456788), false)
	testing.expect_value(t, contains(rb3, 123456789), true)
	testing.expect_value(t, contains(rb3, 123456800), true)
	testing.expect_value(t, contains(rb3, 123456801), false)

	roaring_bitmap_free(&rb1)
	roaring_bitmap_free(&rb2)
	roaring_bitmap_free(&rb3)
}

@(test)
test_errors_thrown :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	// Ensure we don't prefill the packed array with any 0 values
	// after initializing.
	testing.expect_value(t, contains(rb, 0), false)

	ok: bool
	err: Roaring_Error

	// Assert we insert without errors.
	ok, err = add(&rb, 0)
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, len(rb.index), 1)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, err, nil)

	// Attempting to insert again causes an Already_Set_Error to be returned.
	ok, err = add(&rb, 0)
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, len(rb.index), 1)
	testing.expect_value(t, ok, false)
	_, ok = err.(Already_Set_Error_Int)
	testing.expect_value(t, ok, true)

	// // Unsetting works as expected.
	ok, err = remove(&rb, 0)
	testing.expect_value(t, contains(rb, 0), false)
	testing.expect_value(t, len(rb.index), 0)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, err, nil)

	// Unsetting the same value again causes an error.
	ok, err = remove(&rb, 0)
	testing.expect_value(t, contains(rb, 0), false)
	testing.expect_value(t, len(rb.index), 0)
	testing.expect_value(t, ok, false)
	_, ok = err.(Not_Set_Error_Int)
	testing.expect_value(t, ok, true)
}

@(test)
test_bitmap_container_count_runs :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	for i in 0..<10000 {
		if i % 2 == 0 {
			add(&rb, i)
		}
	}

	// Should have 5000 runs, each of length 1.
	bc := rb.index[0].(Bitmap_Container)
	runs := bitmap_container_count_runs(bc)
	testing.expect_value(t, runs, 5000)
}

@(test)
test_should_convert_bitmap_container_to_run_container :: proc(t: ^testing.T) {
	rb, _ := roaring_bitmap_init()
	defer roaring_bitmap_free(&rb)

	for i in 0..<5000 {
		add(&rb, i)
	}

	bc, ok := rb.index[0].(Bitmap_Container)
	testing.expect_value(t, ok, true)

	should := should_convert_container_bitmap_to_run(bc)
	testing.expect_value(t, should, true)
}

@(test)
test_convert_bitmap_to_run_list :: proc(t: ^testing.T) {
	bc, _ := bitmap_container_init()

	bitmap_container_add(&bc, 1)
	bitmap_container_add(&bc, 2)

	bitmap_container_add(&bc, 4)
	bitmap_container_add(&bc, 5)
	bitmap_container_add(&bc, 6)
	bitmap_container_add(&bc, 7)
	bitmap_container_add(&bc, 8)
	bitmap_container_add(&bc, 9)

	for i in 12..<10000 {
		if i % 2 == 0 {
			bitmap_container_add(&bc, u16be(i))
		}
	}

	rc, _ := convert_container_bitmap_to_run(bc)
	defer run_container_free(rc)
	exp_run: Run

	exp_run = Run{start=1, length=2}
	testing.expect_value(t, rc.run_list[0], exp_run)

	exp_run = Run{start=4, length=6}
	testing.expect_value(t, rc.run_list[1], exp_run)

	exp_run = Run{start=12, length=1}
	testing.expect_value(t, rc.run_list[2], exp_run)
}

@(test)
test_convert_bitmap_to_run_list_zero_position :: proc(t: ^testing.T) {
	bc, _ := bitmap_container_init()

	bitmap_container_add(&bc, 0)
	testing.expect_value(t, bitmap_container_contains(bc, 0), true)

	rc, _ := convert_container_bitmap_to_run(bc)
	defer run_container_free(rc)

	exp_run := Run{start=0, length=1}
	testing.expect_value(t, rc.run_list[0], exp_run)
}

@(test)
test_intersection_array_with_run :: proc(t: ^testing.T) {
	// 1 0 0 0 0 0 0 0
	ac, _ := array_container_init()
	defer array_container_free(ac)
	array_container_add(&ac, 0)
	array_container_add(&ac, 4)

	// 1 0 0 1 1 0 0 1
	rc, _ := run_container_init()
	defer run_container_free(rc)
	run_container_add(&rc, 0)
	run_container_add(&rc, 3)
	run_container_add(&rc, 4)
	run_container_add(&rc, 7)

	new_ac, _ := intersection_array_with_run(ac, rc)
	defer array_container_free(new_ac)

	testing.expect_value(t, new_ac.cardinality, 2)
	testing.expect_value(t, array_container_contains(new_ac, 0), true)
	testing.expect_value(t, array_container_contains(new_ac, 1), false)
	testing.expect_value(t, array_container_contains(new_ac, 2), false)
	testing.expect_value(t, array_container_contains(new_ac, 3), false)
	testing.expect_value(t, array_container_contains(new_ac, 4), true)
	testing.expect_value(t, array_container_contains(new_ac, 5), false)
	testing.expect_value(t, array_container_contains(new_ac, 6), false)
	testing.expect_value(t, array_container_contains(new_ac, 7), false)
}

@(test)
test_intersection_bitmap_with_run_array :: proc(t: ^testing.T) {
	// 1 0 0 0 0 0 0 0
	bc, _ := bitmap_container_init()
	defer bitmap_container_free(bc)
	bitmap_container_add(&bc, 0)
	bitmap_container_add(&bc, 4)

	// 1 0 0 1 1 0 0 1
	rc, _ := run_container_init()
	defer run_container_free(rc)
	run_container_add(&rc, 0)
	run_container_add(&rc, 3)
	run_container_add(&rc, 4)
	run_container_add(&rc, 7)

	c, _ := intersection_bitmap_with_run(bc, rc)
	new_ac := c.(Array_Container)
	defer array_container_free(new_ac)

	testing.expect_value(t, new_ac.cardinality, 2)
	testing.expect_value(t, array_container_contains(new_ac, 0), true)
	testing.expect_value(t, array_container_contains(new_ac, 1), false)
	testing.expect_value(t, array_container_contains(new_ac, 2), false)
	testing.expect_value(t, array_container_contains(new_ac, 3), false)
	testing.expect_value(t, array_container_contains(new_ac, 4), true)
	testing.expect_value(t, array_container_contains(new_ac, 5), false)
	testing.expect_value(t, array_container_contains(new_ac, 6), false)
	testing.expect_value(t, array_container_contains(new_ac, 7), false)
}

@(test)
test_intersection_bitmap_with_run_bitmap :: proc(t: ^testing.T) {
	// 1 0 0 0 0 0 0 0
	bc, _ := bitmap_container_init()
	defer bitmap_container_free(bc)
	bitmap_container_add(&bc, 0)
	bitmap_container_add(&bc, 3)
	bitmap_container_add(&bc, 4)
	bitmap_container_add(&bc, 7)

	// 1 0 0 1 1 0 0 1
	rc, _ := run_container_init()
	defer run_container_free(rc)
	for i in 0..<5000 {
		run_container_add(&rc, u16be(i))
	}

	c, _ := intersection_bitmap_with_run(bc, rc)
	new_ac := c.(Array_Container)
	defer array_container_free(new_ac)

	testing.expect_value(t, new_ac.cardinality, 4)
	testing.expect_value(t, array_container_contains(new_ac, 0), true)
	testing.expect_value(t, array_container_contains(new_ac, 1), false)
	testing.expect_value(t, array_container_contains(new_ac, 2), false)
	testing.expect_value(t, array_container_contains(new_ac, 3), true)
	testing.expect_value(t, array_container_contains(new_ac, 4), true)
	testing.expect_value(t, array_container_contains(new_ac, 5), false)
	testing.expect_value(t, array_container_contains(new_ac, 6), false)
	testing.expect_value(t, array_container_contains(new_ac, 7), true)
	testing.expect_value(t, array_container_contains(new_ac, 8), false)
}

@(test)
test_runs_overlap :: proc(t: ^testing.T) {
	testing.expect_value(t, runs_overlap(Run{0, 1}, Run{0, 2}), true)
	testing.expect_value(t, runs_overlap(Run{0, 1}, Run{0, 2}), true)
	testing.expect_value(t, runs_overlap(Run{0, 1}, Run{0, 1}), true)
	testing.expect_value(t, runs_overlap(Run{0, 1}, Run{1, 1}), false)
	testing.expect_value(t, runs_overlap(Run{1, 1}, Run{0, 1}), false)
}

@(test)
test_intersection_run_with_run :: proc(t: ^testing.T) {
	rc1, _ := run_container_init()
	defer run_container_free(rc1)

	rc2, _ := run_container_init()
	defer run_container_free(rc2)

	run_container_add(&rc1, 0)
	run_container_add(&rc1, 2)
	run_container_add(&rc1, 4)
	run_container_add(&rc2, 3)
	run_container_add(&rc2, 4)

	c, _ := intersection_run_with_run(rc1, rc2)
	new_ac, ok := c.(Array_Container)
	defer array_container_free(new_ac)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, new_ac.cardinality, 1)
	testing.expect_value(t, new_ac.packed_array[0], 4)
}

@(test)
test_union_array_with_run :: proc(t: ^testing.T) {
	ac, _ := array_container_init()
	defer array_container_free(ac)

	rc, _ := run_container_init()
	defer run_container_free(rc)

	array_container_add(&ac, 0)
	array_container_add(&ac, 2)
	array_container_add(&ac, 4)
	run_container_add(&rc, 6)
	run_container_add(&rc, 3)
	run_container_add(&rc, 2)

	// Set a lot of bits in the Run_Container so that we remain a Run_Container after
	// the union operation is complete and we don't downgrade to a Array_Container.
	for i in 150..<6000 {
		run_container_add(&rc, u16be(i))
	}

	c, _ := union_array_with_run(ac, rc)
	new_rc, ok := c.(Run_Container)
	defer run_container_free(new_rc)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, container_get_cardinality(new_rc), 5855)
	testing.expect_value(t, len(new_rc.run_list), 4)
	testing.expect_value(t, new_rc.run_list[0], Run{0, 1})
	testing.expect_value(t, new_rc.run_list[1], Run{2, 3})
	testing.expect_value(t, new_rc.run_list[2], Run{6, 1})
	testing.expect_value(t, new_rc.run_list[3], Run{150, 5850})
}

@(test)
test_union_bitmap_with_run :: proc(t: ^testing.T) {
	bc, _ := bitmap_container_init()
	defer bitmap_container_free(bc)

	rc, _ := run_container_init()
	defer run_container_free(rc)

	bitmap_container_add(&bc, 0)
	bitmap_container_add(&bc, 2)
	bitmap_container_add(&bc, 4)
	run_container_add(&rc, 2)
	run_container_add(&rc, 3)
	run_container_add(&rc, 6)

	new_bc, _ := union_bitmap_with_run(bc, rc)
	defer bitmap_container_free(new_bc)

	testing.expect_value(t, new_bc.cardinality, 5)
	testing.expect_value(t, bitmap_container_contains(new_bc, 0), true)
	testing.expect_value(t, bitmap_container_contains(new_bc, 1), false)
	testing.expect_value(t, bitmap_container_contains(new_bc, 2), true)
	testing.expect_value(t, bitmap_container_contains(new_bc, 3), true)
	testing.expect_value(t, bitmap_container_contains(new_bc, 4), true)
	testing.expect_value(t, bitmap_container_contains(new_bc, 5), false)
	testing.expect_value(t, bitmap_container_contains(new_bc, 6), true)
	testing.expect_value(t, bitmap_container_contains(new_bc, 7), false)
	testing.expect_value(t, bitmap_container_contains(new_bc, 8), false)
	testing.expect_value(t, bitmap_container_contains(new_bc, 9), false)
}

@(test)
test_union_run_with_run :: proc(t: ^testing.T) {
	rc1, _ := run_container_init()
	defer run_container_free(rc1)
	run_container_add(&rc1, 6)
	run_container_add(&rc1, 3)
	run_container_add(&rc1, 2)

	rc2, _ := run_container_init()
	defer run_container_free(rc2)
	run_container_add(&rc2, 0)
	run_container_add(&rc2, 4)

	// After running the union on two Run_Container, the result will be
	// downgraded to a Array_Container (new cardinality is <= 4096).
	c, _ := union_run_with_run(rc1, rc2)
	ac, ok := c.(Array_Container)
	defer array_container_free(ac)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, array_container_contains(ac, 0), true)
	testing.expect_value(t, array_container_contains(ac, 1), false)
	testing.expect_value(t, array_container_contains(ac, 2), true)
	testing.expect_value(t, array_container_contains(ac, 3), true)
	testing.expect_value(t, array_container_contains(ac, 4), true)
	testing.expect_value(t, array_container_contains(ac, 5), false)
	testing.expect_value(t, array_container_contains(ac, 6), true)
	testing.expect_value(t, array_container_contains(ac, 7), false)
	testing.expect_value(t, array_container_contains(ac, 8), false)
	testing.expect_value(t, array_container_contains(ac, 9), false)
}
