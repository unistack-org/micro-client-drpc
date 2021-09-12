package drpc

import (
	"github.com/unistack-org/micro/v3/errors"
	"google.golang.org/grpc/status"
)

func microError(err error) error {
	// no error

	if err == nil {
		return nil
	}

	if verr, ok := err.(*errors.Error); ok {
		return verr
	}

	// grpc error
	s, ok := status.FromError(err)
	if !ok {
		return err
	}

	// return first error from details
	if details := s.Details(); len(details) > 0 {
		if verr, ok := details[0].(error); ok {
			return microError(verr)
		}
	}

	// try to decode micro *errors.Error
	if e := errors.Parse(s.Message()); e.Code > 0 {
		return e // actually a micro error
	}

	// fallback
	return &errors.Error{
		Id:     "go.micro.client",
		Code:   int32(s.Code()),
		Detail: s.Message(),
		Status: s.Code().String(),
	}
}
