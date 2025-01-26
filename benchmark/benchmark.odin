package roaring_benchmark

import "core:encoding/json"
import "core:fmt"
import "core:mem"
import "core:os"
import roaring ".."

_main :: proc() {
	bytes, _ := os.read_entire_file_from_filename("benchmark/movies-1910s.json")
	defer delete(bytes)

	data, _ := json.parse(bytes)
	defer json.destroy_value(data)

	movies := make(map[string]roaring.Roaring_Bitmap)
	defer {
		for _, &v in movies {
			roaring.destroy(&v)
		}
		delete(movies)
	}

	for movie, i in data.(json.Array) {
		movie := movie.(json.Object)
		actors := movie["cast"].(json.Array)

		for actor in actors {
			actor := actor.(json.String)
			if !(actor in movies) {
				new_rb, _  := roaring.init()
				movies[actor] = new_rb
			}

			rb := movies[actor]
			roaring.add(&rb, i)
			movies[actor] = rb
		}
	}

	a1 := "Frank Powell"
	rb1 := movies[a1]
	fmt.println(a1, "is in", roaring.get_cardinality(rb1), "movies")
	roaring.print_stats(rb1)
	
	a2 := "Marion Leonard"
	rb2 := movies[a2]
	fmt.println(a2, "is in", roaring.get_cardinality(rb2), "movies")
	roaring.print_stats(rb2)

	roaring.and_inplace(&rb1, rb2)
	in_common := roaring.to_array(rb1)

	fmt.println(roaring.size_in_bytes(rb1))

	defer delete(in_common)
	fmt.println("They both appeared in movie ID:", in_common)
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
