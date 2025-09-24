package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"

	apierrors "github.com/tsamsiyu/themelio/api/internal/api/errors"
	"github.com/tsamsiyu/themelio/api/internal/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
)

// ErrorMapper maps errors from different layers to appropriate HTTP responses
func ErrorMapper(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) > 0 {
			err := c.Errors.Last().Err
			status, body := mapError(err, logger)
			c.JSON(status, body)
		}
	}
}

func mapError(err error, logger *zap.Logger) (int, interface{}) {
	switch e := err.(type) {
	case validator.ValidationErrors:
		var errors []string
		for _, fieldError := range e {
			errors = append(errors, fieldError.Error())
		}
		return http.StatusBadRequest, gin.H{
			"error":   "Validation failed",
			"details": errors,
		}
	case *apierrors.SerializationError:
		logger.Error("API serialization error",
			zap.String("operation", e.Operation),
			zap.Error(e.Err))
		return http.StatusBadRequest, gin.H{"error": "Request serialization failed"}
	case *errors.InvalidInputError:
		return http.StatusBadRequest, gin.H{
			"error": e.Error(),
		}
	case *repository.NotFoundError:
		return http.StatusNotFound, gin.H{"error": e.Error()}
	case *errors.MarshalingError:
		logger.Error("Marshaling error", zap.String("message", e.Message))
		return http.StatusInternalServerError, gin.H{
			"error": "Data processing error",
		}
	default:
		logger.Error("Unhandled error", zap.Error(err))
		return http.StatusInternalServerError, gin.H{"error": "Internal server error"}
	}
}
