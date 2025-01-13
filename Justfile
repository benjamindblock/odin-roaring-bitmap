flags := "-vet -show-timings -strict-style -vet-cast -vet-tabs -vet-using-param -disallow-do -vet-semicolon"
name := "odin-roaring-bitmap"

build:
	@mkdir -p bin
	odin build . -out:bin/{{name}} -debug {{flags}}

test:
	odin test . -out:bin/{{name}} {{flags}}

run: build
	bin/{{name}}

check:
	odin check . {{flags}}
