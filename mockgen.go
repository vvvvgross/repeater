package main

//go:generate mockgen -source=internal/repo/event.go -destination=internal/mocks/repo_mock.go -package=mocks
//go:generate mockgen -source=internal/sender/event.go -destination=internal/mocks/sender_mock.go -package=mocks
