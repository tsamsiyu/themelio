package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/api/errors"
	"github.com/tsamsiyu/themelio/api/internal/repository"
	"github.com/tsamsiyu/themelio/api/internal/service"
)

type ResourceHandler struct {
	logger          *zap.Logger
	resourceService service.ResourceService
	validator       *validator.Validate
}

func NewResourceHandler(
	logger *zap.Logger,
	resourceService service.ResourceService,
	validator *validator.Validate,
) *ResourceHandler {
	return &ResourceHandler{
		logger:          logger,
		resourceService: resourceService,
		validator:       validator,
	}
}

func (h *ResourceHandler) ReplaceResource(c *gin.Context) {
	objectKey := h.getObjectKeyFromParams(c)

	jsonData, err := c.GetRawData()
	if err != nil {
		c.Error(errors.NewSerializationError("reading request body", err))
		return
	}

	err = h.resourceService.ReplaceResource(c.Request.Context(), objectKey, jsonData)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"OK": true})
}

func (h *ResourceHandler) GetResource(c *gin.Context) {
	objectKey := h.getObjectKeyFromParams(c)

	resource, err := h.resourceService.GetResource(c.Request.Context(), objectKey)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, resource)
}

func (h *ResourceHandler) ListResources(c *gin.Context) {
	objectKey := h.getObjectKeyFromParams(c)

	resources, err := h.resourceService.ListResources(c.Request.Context(), objectKey)
	if err != nil {
		c.Error(err)
		return
	}

	response := gin.H{
		"items": resources,
		"total": len(resources),
	}

	c.JSON(http.StatusOK, response)
}

func (h *ResourceHandler) DeleteResource(c *gin.Context) {
	objectKey := h.getObjectKeyFromParams(c)

	err := h.resourceService.DeleteResource(c.Request.Context(), objectKey)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Resource deleted successfully"})
}

func (h *ResourceHandler) getObjectKeyFromParams(c *gin.Context) repository.ObjectKey {
	group := c.Param("group")
	version := c.Param("version")
	kind := c.Param("kind")
	namespace := c.Param("namespace")
	name := c.Param("name")

	return repository.NewObjectKey(group, version, kind, namespace, name)
}
