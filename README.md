# odin-roaring-bitmap
Implementation of 32bit [Roaring Bitmaps](https://roaringbitmap.org) in pure [Odin](https://odin-lang.org).

Based on the following research paper: [https://arxiv.org/pdf/1603.06549](https://arxiv.org/pdf/1603.06549)

**NOTE:** This library is still in an alpha state. The core functionality for roaring bitmaps has been implemented, but many performance optimizations & useful features are still outstanding.

## Example
```odin
rb1, _ := init(context.temp_allocator)
add_many(&rb1, 1, 2, 3, 700123)
fmt.println("Bitmap 1 :", to_array(rb1, context.temp_allocator))

rb2, _ := init(context.temp_allocator)
add_many(&rb2, 0, 4, 6, 700123)
fmt.println("Bitmap 2 :", to_array(rb2, context.temp_allocator))

or_inplace(&rb1, rb2)
fmt.println("OR result:", to_array(rb1, context.temp_allocator))
```

Output:
```
Bitmap 1 : [1, 2, 3, 700123]
Bitmap 2 : [0, 4, 6, 700123]
OR result: [0, 1, 2, 3, 4, 6, 700123]
```

## Running tests
```
$ just test
```

## Procedures
### Creating and destroying a `Roaring_Bitmap`
```
init :: proc(allocator := context.allocator) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

```
roaring_bitmap_destroy :: proc(rb: ^Roaring_Bitmap) {...}
```

```
clone :: proc(rb: Roaring_Bitmap, allocator := context.allocator) -> (new_rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

### Getting a bit
```
select :: proc(rb: Roaring_Bitmap, n: u32) -> int {...}
```

### Set / unset
```
add :: proc(rb: ^Roaring_Bitmap, n: u32) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

```
add_many :: proc(rb: ^Roaring_Bitmap, nums: ..u32) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

```
strict_add :: proc(rb: ^Roaring_Bitmap, n: u32) -> (ok: bool, err: Roaring_Error) {...}
```

```
strict_add_many :: proc(rb: ^Roaring_Bitmap, nums: ..u32) -> (ok: bool, err: Roaring_Error) {...}
```

```
remove :: proc(rb: ^Roaring_Bitmap, n: u32) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

```
remove_many :: proc(rb: ^Roaring_Bitmap, nums: ..u32) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

```
strict_remove :: proc(rb: ^Roaring_Bitmap, n: u32) -> (ok: bool, err: Roaring_Error) {...}
```

```
strict_remove_many :: proc(rb: ^Roaring_Bitmap, nums: ..u32) -> (ok: bool, err: Roaring_Error) {...}
```

```
flip :: proc(rb: Roaring_Bitmap, start: u32, end: u32) -> (new_rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

```
flip_at :: proc(rb: ^Roaring_Bitmap, n: u32) {...}
```

```
flip_range :: proc(rb: ^Roaring_Bitmap, start: u32, end: u32) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

### AND
```
and :: proc(rb1: Roaring_Bitmap, rb2: Roaring_Bitmap, allocator := context.allocator) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

```
and_inplace :: proc(rb1: ^Roaring_Bitmap, rb2: Roaring_Bitmap) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

### OR
```
or :: proc(rb1: Roaring_Bitmap, rb2: Roaring_Bitmap, allocator := context.allocator) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

```
or_inplace :: proc(rb1: ^Roaring_Bitmap, rb2: Roaring_Bitmap) -> (ok: bool, err: runtime.Allocator_Error) {...}
```

### XOR
```
xor :: proc(rb1: Roaring_Bitmap, rb2: Roaring_Bitmap, allocator := context.allocator) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

```
xor_inplace :: proc(rb1: ^Roaring_Bitmap, rb2: Roaring_Bitmap) -> (err: runtime.Allocator_Error) {...}
```

### ANDNOT
```
andnot :: proc(rb1: Roaring_Bitmap, rb2: Roaring_Bitmap, allocator := context.allocator) -> (rb: Roaring_Bitmap, err: runtime.Allocator_Error) {...}
```

```
andnot_inplace :: proc(rb1: ^Roaring_Bitmap, rb2: Roaring_Bitmap) -> (err: runtime.Allocator_Error) {...}
```

### Iterating
```
make_iterator :: proc(rb: ^Roaring_Bitmap) -> Roaring_Bitmap_Iterator {...}
```

```
iterate_set_values :: proc(it: ^Roaring_Bitmap_Iterator) -> (v: u32, index: int, ok: bool) {...}
```

### Optimizing
```
is_optimized :: proc(rb: Roaring_Bitmap) -> bool {...}
```

```
optimize :: proc(rb: ^Roaring_Bitmap) -> (err: runtime.Allocator_Error) {...}
```


### Utilities
```
get_cardinality :: proc(rb: Roaring_Bitmap) -> (cardinality: int) {...}
```

```
to_array :: proc(rb: Roaring_Bitmap, allocator := context.allocator) -> [dynamic]u32 {...}
```

```
contains :: proc(rb: Roaring_Bitmap, n: u32) -> (found: bool) {...}
```

```
estimate_size_in_bytes :: proc(rb: Roaring_Bitmap) -> (size: int) {...}
```

```
print_stats :: proc(rb: Roaring_Bitmap) {...}
```

```
size_in_bytes :: proc(rb: Roaring_Bitmap) -> (size: int) {...}
```

### Writing / loading from file
```
deserialize :: proc(filepath: string, allocator := context.allocator) -> (rb: Roaring_Bitmap, err: Roaring_Error) {...}
```

```
serialize :: proc(filepath: string, rb: Roaring_Bitmap) -> (bytes_written: int, err: Roaring_Error) {...}
```
