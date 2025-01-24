package roaring

import "base:builtin"
import "base:runtime"
import "core:slice"

@(private, require_results)
run_container_init :: proc(
	allocator := context.allocator
) -> (Run_Container, runtime.Allocator_Error) {
	run_list, err := make(Run_List, allocator)
	rc := Run_Container{run_list}

	return rc, err
}

@(private)
run_container_free :: proc(rc: Run_Container) {
	delete(rc.run_list)
}

// Sets a value in a Run_List.
// TODO: Cleanup and unify with run_container_remove and flip_within_container.
@(private)
run_container_add :: proc(
	rc: ^Run_Container,
	n: u16be,
) -> (ok: bool, err: runtime.Allocator_Error) {
	n := int(n)

	// If the Run_List is empty, then create the first Run and add it to the list.
	if len(rc.run_list) == 0 {
		new_run := Run{start=n, length=1}
		append(&rc.run_list, new_run) or_return
		return true, nil
	}

	i, found := run_list_could_contain_binary_search(rc.run_list, n)

	// If we did not find a Run that could contain this N-value, start a new Run
	// and add it to the Run_List.
	if !found {
		new_run := Run{start = n, length = 1}
		inject_at(&rc.run_list, i, new_run) or_return
		return true, nil
	}

	// Otherwise, we can expand the Run that was found either forwards or backwards
	// to include the N-value.
	run_to_expand := &rc.run_list[i]

	// Expand the matching Run backwards if needed.
	if run_to_expand.start - 1 == n {
		run_to_expand.start -= 1
		run_to_expand.length += 1

		// Merge with the previous run if we need to.
		if i - 1 >= 0 {
			prev_run := rc.run_list[i-1]
			if run_to_expand.start == run_end_position(prev_run) {
				run_to_expand.length += prev_run.length
				run_to_expand.start = prev_run.start
				ordered_remove(&rc.run_list, i-1)
			}
		}

	// Expand a Run forwards.
	} else if run_end_position(run_to_expand^) == n {
		run_to_expand.length += 1

		// Merge with the next run if we need to.
		if i + 1 < len(rc.run_list) {
			next_run := rc.run_list[i+1]
			if run_end_position(run_to_expand^) == next_run.start {
				run_to_expand.length += next_run.length
				ordered_remove(&rc.run_list, i+1)
			}
		}
	}

	return true, nil
}

// Cases:
// 1. Standalone Run -- remove
// 2. Value at beginning of run -- increment start by 1 and decrease length by 1
// 3. Value at end of run -- decrease length by 1
// 4. Value in middle of run -- split Run into two Runs
@(private)
run_container_remove :: proc(
	rc: ^Run_Container,
	n: u16be,
) -> (ok: bool, err: runtime.Allocator_Error) {
	n := int(n)

	// If we don't find an exact match, we have a problem!
	// That means the N-value is not actually in the Run_List.
	i, exact_match := run_list_binary_search(rc.run_list, n)
	if !exact_match {
		return false, nil
	}

	run_to_check := &rc.run_list[i]

	// 1. Standalone Run -- remove
	if run_to_check.length == 1 {
		ordered_remove(&rc.run_list, i)	

	// 2. Value at beginning of run -- increment start by 1 and decrease length by 1
	} else if run_to_check.start == n {
		run_to_check.start += 1
		run_to_check.length -= 1

	// 3. Value at end of run -- decrease length by 1
	} else if run_end_position(run_to_check^) - 1 == n {
		run_to_check.length -= 1

	// 4. Value in middle of run -- split Run into two Runs
	} else {
		run1 := Run{
			start = run_to_check.start,
			length = (n - run_to_check.start),
		}
		run2 := run_to_check

		run2.start = n + 1
		run2.length = run2.length - (run2.start - run1.start)
		inject_at(&rc.run_list, i, run1) or_return
	}

	return true, nil
}

// Checks to see if a value is set in a Run_Container.
@(private)
run_container_contains :: proc(rc: Run_Container, n: u16be) -> bool {
	if len(rc.run_list) == 0 {
		return false
	}

	_, found := run_list_binary_search(rc.run_list, int(n))
	return found
}

// Finds the cardinality of a Run_Container by summing the length of each run.
@(private)
run_container_get_cardinality :: proc(rc: Run_Container) -> (acc: int) {
	rl := rc.run_list

	if len(rl) == 0 {
		return 0
	}

	for run in rc.run_list {
		acc += run.length
	}

	return acc
}

// Finds the end position of the given Run in the container (exclusive).
@(private)
run_end_position :: proc(run: Run) -> int {
	return run.start + run.length
}

// Checks if two Run structs overlap at all.
@(private)
runs_overlap :: proc(r1: Run, r2: Run) -> bool {
	start1 := r1.start
	start2 := r2.start
	end1 := run_end_position(r1)
	end2 := run_end_position(r2)

	return start1 < end2 && start2 < end1
}

// Finds the range (inclusive at both ends) that two Run
// structs are overlapping at.
@(private)
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

run_contains :: proc(r: Run, n: int) -> bool {
	return n >= r.start && n < run_end_position(r)
}

// Checks if a given run *could* contain a given N-value by either
// expansion forwards or backwards.
//
// Eg., run_could_contain(Run{2, 1}, 1) => true (as Run{1, 2} would
// be the new Run that contains the N-value).
run_could_contain :: proc(r: Run, n: int) -> bool {
	return n >= (r.start - 1) && n <= run_end_position(r)
}

// Searches for a given N-value in a Run_List.
// - true if the value is inside a Run in the Run_List.
// Otherwise returns the position in the Run_List where a value
// could be added.
run_list_binary_search :: proc(rl: Run_List, n: int) -> (int, bool) {
	cmp := proc(r: Run, n: int) -> (res: slice.Ordering) {
		if run_contains(r, n) {
			res = .Equal
		} else if r.start > n {
			res = .Greater
		} else {
			res = .Less
		}

		return res
	}

	return slice.binary_search_by(rl[:], n, cmp)
}

// Searches for a Run in the Run_List that *could* contain the given N-value.
// This means that we calculate the (start - 1) and (end + 1) of each Run before checking
// if the N-value is inside it. If found, then that Run either:
// - Currently contains the N-value
// - Could be expanded forward or backwards to include it
run_list_could_contain_binary_search :: proc(rl: Run_List, n: int) -> (int, bool) {
	cmp := proc(r: Run, n: int) -> (res: slice.Ordering) {
		if run_could_contain(r, n) {
			res = .Equal
		} else if r.start > n {
			res = .Greater
		} else {
			res = .Less
		}

		return res
	}

	return slice.binary_search_by(rl[:], n, cmp)
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
@(private)
run_container_and_run_container :: proc(
	rc1: Run_Container,
	rc2: Run_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	c = run_container_init(allocator) or_return
	new_rc := c.(Run_Container)
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
				run_container_add(&new_rc, u16be(n)) or_return
			}

			if run_end_position(run1) < run_end_position(run2) {
				i += 1
			} else if run_end_position(run2) < run_end_position(run1) {
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

	c = container_convert_to_optimal(new_rc, allocator) or_return
	return c, nil
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
@(private)
run_container_or_run_container :: proc(
	rc1: Run_Container,
	rc2: Run_Container,
	allocator := context.allocator,
) -> (c: Container, err: runtime.Allocator_Error) {
	c = run_container_init(allocator) or_return
	new_rc := c.(Run_Container)

	// FIXME: Can any of this be optimized?
	for run in rc1.run_list {
		for i := run.start; i < run_end_position(run); i += 1 {
			run_container_add(&new_rc, u16be(i)) or_return
		}
	}

	// FIXME: Can any of this be optimized?
	for run in rc2.run_list {
		for i := run.start; i < run_end_position(run); i += 1 {
			run_container_add(&new_rc, u16be(i)) or_return
		}
	}


	c = container_convert_to_optimal(new_rc, allocator) or_return
	return c, nil
}

// Run_Container => Array_Container
@(private)
run_container_convert_to_array_container :: proc(
	rc: Run_Container,
	allocator := context.allocator,
) -> (ac: Array_Container, err: runtime.Allocator_Error) {
	ac = array_container_init(allocator) or_return

	for run in rc.run_list {
		start := run.start
		for i := 0; i < run.length; i += 1 {
			v := u16be(start + i)
			array_container_add(&ac, v) or_return
		}
	}

	run_container_free(rc)
	return ac, nil
}

// Run_Container => Bitmap_Container
@(private)
run_container_convert_to_bitmap_container :: proc(
	rc: Run_Container,
	allocator := context.allocator,
) -> (bc: Bitmap_Container, err: runtime.Allocator_Error) {
	bc = bitmap_container_init(allocator) or_return

	for run in rc.run_list {
		start := run.start
		for i := 0; i < run.length; i += 1 {
			v := u16be(start + i)
			bitmap_container_add(&bc, v) or_return
		}
	}

	run_container_free(rc)
	return bc, nil
}
