package roaring

import "base:runtime"
import "core:fmt"
import "core:slice"
import "core:testing"

@(test)
test_flip_at_and_select :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	testing.expect_value(t, select(rb, 0), 0)
	testing.expect_value(t, select(rb, 2), 0)

	flip_at(&rb, 2)
	testing.expect_value(t, select(rb, 0), 0)
	testing.expect_value(t, select(rb, 2), 1)
}

@(test)
test_make_iterator :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	add(&rb, 2)
}

@(test)
test_iterate_set_values_arrays :: proc(t: ^testing.T) {
	rb, _ := init()
	it := make_iterator(&rb)
	defer destroy(&rb)

	add(&rb, 2)
	add(&rb, 230000324)
	add(&rb, 230000325)
	add(&rb, 230000326)
	add(&rb, 300100)

	for v, i in iterate_set_values(&it) {
		switch i {
		case 0:
			testing.expect_value(t, v, 2)
		case 1:
			testing.expect_value(t, v, 300100)
		case 2:
			testing.expect_value(t, v, 230000324)
		case 3:
			testing.expect_value(t, v, 230000325)
		case 4:
			testing.expect_value(t, v, 230000326)
		}
	}
}

@(test)
test_clone :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	add(&rb, 2)
	testing.expect_value(t, select(rb, 2), 1)

	rb2, _ := clone(rb)
	defer destroy(&rb2)
	testing.expect_value(t, select(rb2, 2), 1)
}

@(test)
test_setting_values_works_for_array :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

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
	for k, v in rb.containers {
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
	for _, v in rb.containers {
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
	rb, _ := init()
	defer destroy(&rb)

	for i in 0..<4096 {
		add(&rb, i)
	}
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, contains(rb, 4095), true)

	count := 0
	container: Container
	for _, v in rb.containers {
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
	for _, v in rb.containers {
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
	for _, v in rb.containers {
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
	testing.expect_value(t, rc.run_list[0], Run{0, 0})
	testing.expect_value(t, rc.run_list[1], Run{2, 2})

	run_container_add(&rc, 5)
	testing.expect_value(t, len(rc.run_list), 2)
	testing.expect_value(t, rc.run_list[0], Run{0, 0})
	testing.expect_value(t, rc.run_list[1], Run{2, 3})

	run_container_add(&rc, 1)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, rc.run_list[0], Run{0, 5})

	run_container_remove(&rc, 1)
	testing.expect_value(t, rc.run_list[0], Run{0, 0})
	testing.expect_value(t, rc.run_list[1], Run{2, 3})

	run_container_add(&rc, 1)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, rc.run_list[0], Run{0, 5})

	run_container_remove(&rc, 0)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, rc.run_list[0], Run{1, 4})
}

@(test)
test_converting_from_bitmap_to_run_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	// Confirm all 5000 bits are set in the Bitmap_Container.
	for i in 0..<5000 {
		add(&rb, i)
	}
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, contains(rb, 4999), true)
	container := rb.containers[0]
	bc, bc_ok := container.(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 5000)
	testing.expect_value(t, bitmap_container_should_convert_to_run(bc), true)

	optimize(&rb)
	container = rb.containers[0]
	rc, rc_ok := container.(Run_Container)
	testing.expect_value(t, rc_ok, true)
	testing.expect_value(t, run_container_get_cardinality(rc), 5000)

	exp_run := Run{start=0, length=4999}
	testing.expect_value(t, rc.run_list[0], exp_run)
}

@(test)
test_converting_from_run_to_bitmap_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	// Confirm all 6000 bits are set in the Bitmap_Container.
	for i in 0..<6000 {
		add(&rb, i)
	}
	optimize(&rb)

	container := rb.containers[0]
	rc, rc_ok := container.(Run_Container)
	testing.expect_value(t, rc_ok, true)
	testing.expect_value(t, run_container_get_cardinality(rc), 6000)
	testing.expect_value(t, len(rc.run_list), 1)

	for i in 0..=4094 {
		if i % 2 == 0 {
			remove(&rb, i)
		}
	}

	container = rb.containers[0]
	bc, bc_ok := container.(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 3952)
	testing.expect_value(t, bitmap_container_count_runs(bc), 2048)
}

@(test)
test_multiple_array_containers :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	add(&rb, 0)
	add(&rb, 1)
	add(&rb, 123456789)

	testing.expect_value(t, len(rb.containers), 2)

	ac1, ok1 := rb.containers[most_significant(0)].(Array_Container)
	testing.expect_value(t, ok1, true)
	testing.expect_value(t, ac1.cardinality, 2)

	ac2, ok2 := rb.containers[most_significant(123456789)].(Array_Container)
	testing.expect_value(t, ok2, true)
	testing.expect_value(t, ac2.cardinality, 1)
}

@(test)
test_and_array :: proc(t: ^testing.T) {
	rb1, _ := init()
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	add(&rb2, 1)

	rb3, _ := and(rb1, rb2)
	testing.expect_value(t, contains(rb3, 0), false)
	testing.expect_value(t, contains(rb3, 1), true)

	destroy(&rb1)
	destroy(&rb2)
	destroy(&rb3)
}

@(test)
test_and_inplace_array :: proc(t: ^testing.T) {
	rb1, _ := init()
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	add(&rb2, 1)

	and_inplace(&rb1, rb2)
	testing.expect_value(t, contains(rb1, 0), false)
	testing.expect_value(t, contains(rb1, 1), true)

	destroy(&rb1)
	destroy(&rb2)
}

@(test)
test_andnot_array :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	add(&rb2, 0)
	add(&rb2, 2)

	rb3, _ := andnot(rb1, rb2)
	defer destroy(&rb3)
	testing.expect_value(t, contains(rb3, 0), false)
	testing.expect_value(t, contains(rb3, 1), true)
	testing.expect_value(t, contains(rb3, 2), false)
	testing.expect_value(t, contains(rb3, 3), false)
}

@(test)
test_andnot_inplace_array :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	add(&rb1, 0)
	add(&rb2, 0)
	add(&rb2, 2)

	andnot_inplace(&rb1, rb2)
	testing.expect_value(t, contains(rb1, 0), false)
	testing.expect_value(t, contains(rb1, 1), true)
	testing.expect_value(t, contains(rb1, 2), false)
	testing.expect_value(t, contains(rb1, 3), false)
}

@(test)
test_and_array_and_bitmap :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	for i in 0..=4096 {
		add(&rb2, i)
	}

	rb3, _ := and(rb1, rb2)
	defer destroy(&rb3)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 1), true)
	testing.expect_value(t, contains(rb3, 2), false)
	testing.expect_value(t, contains(rb3, 4096), false)
}

@(test)
test_and_bitmap :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	for i in 0..=4096 {
		add(&rb1, i)
	}

	rb2, _ := init()
	defer destroy(&rb2)
	for i in 4096..=9999 {
		add(&rb2, i)
	}

	rb3, _ := and(rb1, rb2)
	defer destroy(&rb3)
	testing.expect_value(t, len(rb3.containers), 1)
	testing.expect_value(t, contains(rb3, 4095), false)
	testing.expect_value(t, contains(rb3, 4096), true)
	testing.expect_value(t, contains(rb3, 4097), false)
}

@(test)
test_or_array :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	add(&rb2, 1)

	rb3, _ := or(rb1, rb2)
	defer destroy(&rb3)
	testing.expect_value(t, len(rb3.containers), 1)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 1), true)
}

@(test)
test_or_inplace_array :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	add(&rb2, 1)

	or_inplace(&rb1, rb2)
	testing.expect_value(t, len(rb1.containers), 1)
	testing.expect_value(t, contains(rb1, 0), true)
	testing.expect_value(t, contains(rb1, 1), true)
}

@(test)
test_or_array_and_bitmap :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	for i in 0..=4096 {
		add(&rb2, i)
	}

	rb3, _ := or(rb1, rb2)
	defer destroy(&rb3)
	testing.expect_value(t, len(rb3.containers), 1)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 1), true)
	testing.expect_value(t, contains(rb3, 2), true)
	testing.expect_value(t, contains(rb3, 4096), true)
	testing.expect_value(t, contains(rb3, 4097), false)
}

@(test)
test_or_inplace_array_and_bitmap :: proc(t: ^testing.T) {
	rb1, _ := init()
	defer destroy(&rb1)
	add(&rb1, 0)
	add(&rb1, 1)

	rb2, _ := init()
	defer destroy(&rb2)
	for i in 0..=4096 {
		add(&rb2, i)
	}

	or_inplace(&rb1, rb2)
	testing.expect_value(t, len(rb1.containers), 1)
	testing.expect_value(t, contains(rb1, 0), true)
	testing.expect_value(t, contains(rb1, 1), true)
	testing.expect_value(t, contains(rb1, 2), true)
	testing.expect_value(t, contains(rb1, 4096), true)
	testing.expect_value(t, contains(rb1, 4097), false)
}

@(test)
test_or_bitmap :: proc(t: ^testing.T) {
	rb1, _ := init()
	for i in 0..=4096 {
		add(&rb1, i)
	}

	rb2, _ := init()
	for i in 123456789..=123456800 {
		add(&rb2, i)
	}

	rb3, _ := or(rb1, rb2)
	testing.expect_value(t, len(rb3.containers), 2)
	testing.expect_value(t, contains(rb3, 0), true)
	testing.expect_value(t, contains(rb3, 4095), true)
	testing.expect_value(t, contains(rb3, 4096), true)
	testing.expect_value(t, contains(rb3, 4097), false)
	testing.expect_value(t, contains(rb3, 123456788), false)
	testing.expect_value(t, contains(rb3, 123456789), true)
	testing.expect_value(t, contains(rb3, 123456800), true)
	testing.expect_value(t, contains(rb3, 123456801), false)

	destroy(&rb1)
	destroy(&rb2)
	destroy(&rb3)
}

@(test)
test_strict_methods :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	// Ensure we don't prefill the packed array with any 0 values
	// after initializing.
	testing.expect_value(t, contains(rb, 0), false)

	ok: bool
	err: Roaring_Error

	// Assert we insert without errors.
	ok, err = strict_add(&rb, 0)
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, len(rb.containers), 1)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, err, runtime.Allocator_Error.None)

	// Attempting to insert again causes an Already_Set_Error to be returned.
	ok, err = strict_add(&rb, 0)
	testing.expect_value(t, contains(rb, 0), true)
	testing.expect_value(t, len(rb.containers), 1)
	testing.expect_value(t, ok, false)
	_, ok = err.(Already_Set_Error)
	testing.expect_value(t, ok, true)

	// Unsetting works as expected.
	ok, err = strict_remove(&rb, 0)
	testing.expect_value(t, contains(rb, 0), false)
	testing.expect_value(t, len(rb.containers), 0)
	testing.expect_value(t, ok, true)
	testing.expect_value(t, err, runtime.Allocator_Error.None)

	// Unsetting the same value again causes an error.
	ok, err = strict_remove(&rb, 0)
	testing.expect_value(t, contains(rb, 0), false)
	testing.expect_value(t, len(rb.containers), 0)
	testing.expect_value(t, ok, false)
	_, ok = err.(Not_Set_Error)
	testing.expect_value(t, ok, true)
}

@(test)
test_bitmap_container_count_runs :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	for i in 0..<10000 {
		if i % 2 == 0 {
			add(&rb, i)
		}
	}

	// Should have 5000 runs, each of length 1.
	bc := rb.containers[0].(Bitmap_Container)
	runs := bitmap_container_count_runs(bc)
	testing.expect_value(t, runs, 5000)
}

@(test)
test_should_convert_bitmap_container_to_run_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	for i in 0..<5000 {
		add(&rb, i)
	}

	bc, ok := rb.containers[0].(Bitmap_Container)
	testing.expect_value(t, ok, true)

	should := bitmap_container_should_convert_to_run(bc)
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

	rc, _ := bitmap_container_convert_to_run_container(bc)
	defer run_container_free(rc)
	exp_run: Run

	exp_run = Run{start=1, length=1}
	testing.expect_value(t, rc.run_list[0], exp_run)

	exp_run = Run{start=4, length=5}
	testing.expect_value(t, rc.run_list[1], exp_run)

	exp_run = Run{start=12, length=0}
	testing.expect_value(t, rc.run_list[2], exp_run)
}

@(test)
test_convert_bitmap_to_run_list_zero_position :: proc(t: ^testing.T) {
	bc, _ := bitmap_container_init()

	bitmap_container_add(&bc, 0)
	testing.expect_value(t, bitmap_container_contains(bc, 0), true)

	rc, _ := bitmap_container_convert_to_run_container(bc)
	defer run_container_free(rc)

	exp_run := Run{start=0, length=0}
	testing.expect_value(t, rc.run_list[0], exp_run)
}

@(test)
test_and_array_with_run :: proc(t: ^testing.T) {
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

	new_ac, _ := array_container_and_run_container(ac, rc)
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
test_and_bitmap_with_run_array :: proc(t: ^testing.T) {
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

	c, _ := bitmap_container_and_run_container(bc, rc)
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
test_and_bitmap_with_run_bitmap :: proc(t: ^testing.T) {
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

	c, _ := bitmap_container_and_run_container(bc, rc)
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
	testing.expect_value(t, runs_overlap(Run{0, 0}, Run{1, 1}), false)
	testing.expect_value(t, runs_overlap(Run{1, 1}, Run{0, 0}), false)
}

@(test)
test_and_run_with_run :: proc(t: ^testing.T) {
	rc1, _ := run_container_init()
	defer run_container_free(rc1)

	rc2, _ := run_container_init()
	defer run_container_free(rc2)

	run_container_add(&rc1, 0)
	run_container_add(&rc1, 2)
	run_container_add(&rc1, 4)
	run_container_add(&rc2, 3)
	run_container_add(&rc2, 4)

	c, _ := run_container_and_run_container(rc1, rc2)
	new_ac, ok := c.(Array_Container)
	defer array_container_free(new_ac)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, new_ac.cardinality, 1)
	testing.expect_value(t, new_ac.packed_array[0], 4)
}

@(test)
test_or_array_with_run :: proc(t: ^testing.T) {
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

	c, _ := array_container_or_run_container(ac, rc)
	new_rc, ok := c.(Run_Container)
	defer run_container_free(new_rc)

	testing.expect_value(t, ok, true)
	testing.expect_value(t, container_get_cardinality(new_rc), 5855)
	testing.expect_value(t, len(new_rc.run_list), 4)
	testing.expect_value(t, new_rc.run_list[0], Run{0, 0})
	testing.expect_value(t, new_rc.run_list[1], Run{2, 2})
	testing.expect_value(t, new_rc.run_list[2], Run{6, 0})
	testing.expect_value(t, new_rc.run_list[3], Run{150, 5849})
}

@(test)
test_bitmap_container_or_run_container :: proc(t: ^testing.T) {
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

	new_bc, _ := bitmap_container_or_run_container(bc, rc)
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
test_or_run_with_run :: proc(t: ^testing.T) {
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
	c, _ := run_container_or_run_container(rc1, rc2)
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

@(test)
test_container_is_full :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	// Should end up with two containers, the first one is full at
	// 65536 values and the second one half full.
	for i in 0..=100000 {
		add(&rb, i)
	}

	// Check the logic for Bitmap_Container.
	bc1, bc1_ok := rb.containers[0].(Bitmap_Container)
	testing.expect_value(t, bc1_ok, true)
	testing.expect_value(t, bc1.cardinality, 65536)
	testing.expect_value(t, container_is_full(bc1), true)

	bc, bc_ok := rb.containers[1].(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 34465)
	testing.expect_value(t, container_is_full(bc), false)

	// Check the logic for Run_Container.
	optimize(&rb)
	rc1, rc1_ok := rb.containers[0].(Run_Container)
	testing.expect_value(t, rc1_ok, true)
	testing.expect_value(t, len(rc1.run_list), 1)
	testing.expect_value(t, container_is_full(rc1), true)
	testing.expect_value(t, rc1.run_list[0], Run{0, 65535})

	rc2, rc2_ok := rb.containers[1].(Run_Container)
	testing.expect_value(t, rc2_ok, true)
	testing.expect_value(t, len(rc2.run_list), 1)
	testing.expect_value(t, container_is_full(rc2), false)
	testing.expect_value(t, rc2.run_list[0], Run{0, 34464})
}

@(test)
test_flip_inplace_with_empty_roaring_bitmap :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	flip_inplace(&rb, 0, 1000)

	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)

	rc1, rc1_ok := rb.containers[rb.cindex[0]].(Run_Container)
	testing.expect_value(t, rc1_ok, true)
	testing.expect_value(t, container_get_cardinality(rc1), 1001)
	testing.expect_value(t, len(rc1.run_list), 1)
	testing.expect_value(t, rc1.run_list[0], Run{0, 1000})
}

@(test)
test_flip_inplace_with_full_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	for i in 0..<65536 {
		add(&rb, i)
	}

	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)
	c := rb.containers[rb.cindex[0]]
	testing.expect_value(t, container_is_full(c), true)
	testing.expect_value(t, container_get_cardinality(c), 65536)

	flip_inplace(&rb, 0, 65535)
	testing.expect_value(t, len(rb.cindex), 0)
	testing.expect_value(t, len(rb.containers), 0)
}

@(test)
test_flip_inplace_array_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	add(&rb, 3)
	add(&rb, 5)

	flip_inplace(&rb, 1, 7)
	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)
	ac, ac_ok := rb.containers[rb.cindex[0]].(Array_Container)
	testing.expect_value(t, ac_ok, true)
	testing.expect_value(t, ac.cardinality, 5)
	equal := slice.equal(ac.packed_array[:], []u16be{1, 2, 4, 6, 7})
	testing.expect_value(t, len(ac.packed_array), 5)
	testing.expect_value(t, equal, true)

	// Flip back and assert is correct.
	flip_inplace(&rb, 1, 7)
	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)
	ac, ac_ok = rb.containers[rb.cindex[0]].(Array_Container)
	testing.expect_value(t, ac_ok, true)
	testing.expect_value(t, ac.cardinality, 2)
	equal = slice.equal(ac.packed_array[:], []u16be{3, 5})
	testing.expect_value(t, len(ac.packed_array), 2)
	testing.expect_value(t, equal, true)
}

@(test)
test_flip_inplace_array_container_remove :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	add(&rb, 3)
	add(&rb, 4)
	flip_inplace(&rb, 3, 4)
	testing.expect_value(t, len(rb.cindex), 0)
	testing.expect_value(t, len(rb.containers), 0)

	flip_inplace(&rb, 3, 4)
	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)

	// We will default to a Run container when flipping all zeds to one.
	rc, rc_ok := rb.containers[rb.cindex[0]].(Run_Container)
	testing.expect_value(t, rc_ok, true)
	testing.expect_value(t, len(rc.run_list), 1)
	testing.expect_value(t, rc.run_list[0], Run{3, 1})
}

@(test)
test_flip_inplace_bitmap_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	for i in 0..<5000 {
		add(&rb, i)
	}

	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)
	bc, bc_ok := rb.containers[rb.cindex[0]].(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 5000)

	// Flip from 6 to 9 to ensure we cross bytes.
	flip_inplace(&rb, 6, 9)
	bc, bc_ok = rb.containers[rb.cindex[0]].(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 4996)
	testing.expect_value(t, contains(rb, 5), true)
	testing.expect_value(t, contains(rb, 6), false)
	testing.expect_value(t, contains(rb, 7), false)
	testing.expect_value(t, contains(rb, 8), false)
	testing.expect_value(t, contains(rb, 9), false)
	testing.expect_value(t, contains(rb, 10), true)

	flip_inplace(&rb, 7, 8)
	bc, bc_ok = rb.containers[rb.cindex[0]].(Bitmap_Container)
	testing.expect_value(t, bc_ok, true)
	testing.expect_value(t, bc.cardinality, 4998)
	testing.expect_value(t, contains(rb, 5), true)
	testing.expect_value(t, contains(rb, 6), false)
	testing.expect_value(t, contains(rb, 7), true)
	testing.expect_value(t, contains(rb, 8), true)
	testing.expect_value(t, contains(rb, 9), false)
	testing.expect_value(t, contains(rb, 10), true)
}

@(test)
test_flip_inplace_run_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	for i in 0..<60000 {
		if i > 0 && i < 4 {
			continue
		}
		add(&rb, i)
	}
	optimize(&rb)

	// [Run{start = 0, length = 1}, Run{start = 4, length = 59996}]
	// =>  [Run{start = 0, length = 4}, Run{start = 6, length = 59991}, Run{start = 60000, length = 4}]}]
	flip_inplace(&rb, 1, 5)
	flip_inplace(&rb, 59997, 60003)

	rc, rc_ok := rb.containers[rb.cindex[0]].(Run_Container)
	testing.expect_value(t, rc_ok, true)
	testing.expect_value(t, len(rc.run_list), 3)
	testing.expect_value(t, rc.run_list[0], Run{0, 3})
	testing.expect_value(t, rc.run_list[1], Run{6, 59990})
	testing.expect_value(t, rc.run_list[2], Run{60000, 3})
}

@(test)
test_flip_array_container :: proc(t: ^testing.T) {
	rb, _ := init()
	defer destroy(&rb)

	add(&rb, 3)
	add(&rb, 5)

	new_rb, err := flip(rb, 1, 7)
	defer destroy(&new_rb)

	// Assert the new Roaring_Bitmap has the flipped data.
	testing.expect_value(t, err, nil)
	testing.expect_value(t, len(new_rb.cindex), 1)
	testing.expect_value(t, len(new_rb.containers), 1)
	ac, ac_ok := new_rb.containers[new_rb.cindex[0]].(Array_Container)
	testing.expect_value(t, ac_ok, true)
	testing.expect_value(t, ac.cardinality, 5)
	equal := slice.equal(ac.packed_array[:], []u16be{1, 2, 4, 6, 7})
	testing.expect_value(t, len(ac.packed_array), 5)
	testing.expect_value(t, equal, true)

	// Assert the original Roaring_Bitmap is unchanged.
	testing.expect_value(t, len(rb.cindex), 1)
	testing.expect_value(t, len(rb.containers), 1)
	ac, ac_ok = rb.containers[rb.cindex[0]].(Array_Container)
	testing.expect_value(t, ac_ok, true)
	testing.expect_value(t, ac.cardinality, 2)
	equal = slice.equal(ac.packed_array[:], []u16be{3, 5})
	testing.expect_value(t, len(ac.packed_array), 2)
	testing.expect_value(t, equal, true)
}

// Ref: https://github.com/RoaringBitmap/RoaringFormatSpec/tree/master/testdata
@(test)
test_serialization_and_deserialization :: proc(t: ^testing.T) {
	rb, _ := deserialize("test_files/bitmapwithoutruns.bin")
	defer destroy(&rb)

	for k := 0; k < 100000; k+= 1000 {
		testing.expect_value(t, contains(rb, k), true)
	}

	for k := 100000; k < 200000; k += 1 {
		testing.expect_value(t, contains(rb, k*3), true)
	}

	for k := 700000; k < 800000; k += 1{
		testing.expect_value(t, contains(rb, k), true)
	}

	serialize("tmp/out.txt", rb)
	rb2, _ := deserialize("tmp/out.txt")
	defer destroy(&rb2)

	for k := 0; k < 100000; k+= 1000 {
		testing.expect_value(t, contains(rb2, k), true)
	}

	for k := 100000; k < 200000; k += 1 {
		testing.expect_value(t, contains(rb2, k*3), true)
	}

	for k := 700000; k < 800000; k += 1{
		testing.expect_value(t, contains(rb2, k), true)
	}
}
