package retrier

import "time"

type Try func() error

func Operation(retriers uint, try Try) error {
	timeout := 50 * time.Millisecond

	var err error

	for range retriers {
		err = try()
		if err == nil {
			return nil
		}

		time.Sleep(timeout)

		timeout *= 2
	}

	return err
}
