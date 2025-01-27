package roaring

// import "core:fmt"
import "core:os"

@(private)
handle_from_file :: proc(filepath: string) -> (fd: os.Handle, err: os.Error) {
	flags: int = os.O_WRONLY | os.O_CREATE

	mode: int = 0
	when os.OS == .Linux || os.OS == .Darwin {
		// NOTE(justasd): 644 (owner read, write; group read; others read)
		mode = os.S_IRUSR | os.S_IWUSR | os.S_IRGRP | os.S_IROTH
	}

	fd = os.open(filepath, flags, mode) or_return
	return fd, nil
}

serialize :: proc() -> Roaring_Error {
	fh := handle_from_file("foo.txt") or_return

	s := "hello!\n"
	os.write(fh, transmute([]byte)s)

	s = "goodbye!\n"
	os.write(fh, transmute([]byte)s)

	return nil
}
