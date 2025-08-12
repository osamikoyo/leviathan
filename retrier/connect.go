package retrier

import "time"

func Connect[T any](retriers uint, connector func() (T, error)) (T, error) {
	var (
		value T
		err   error
	)

	timeout := 50 * time.Millisecond

	for range retriers {
		value, err = connector()
		if err == nil {
			return value, nil
		}

		time.Sleep(timeout)

		timeout *= 2
	}

	return value, err
}
