.PHONY: build install clean

build:
	go build -o picoman ./cmd/picoman

install:
	go install ./cmd/picoman

clean:
	rm -f picoman
