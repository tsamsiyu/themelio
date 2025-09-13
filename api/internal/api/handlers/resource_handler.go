package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/tsamsiyu/themelio/api/internal/api/errors"
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
	gvk := h.getGVKFromParams(c)

	jsonData, err := c.GetRawData()
	if err != nil {
		c.Error(errors.NewSerializationError("reading request body", err))
		return
	}

	err = h.resourceService.ReplaceResource(c.Request.Context(), gvk, jsonData)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"OK": true})
}

func (h *ResourceHandler) GetResource(c *gin.Context) {
	gvk := h.getGVKFromParams(c)
	namespace := c.Param("namespace")
	name := c.Param("name")

	resource, err := h.resourceService.GetResource(c.Request.Context(), gvk, namespace, name)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, resource)
}

func (h *ResourceHandler) ListResources(c *gin.Context) {
	gvk := h.getGVKFromParams(c)
	namespace := c.Param("namespace")

	resources, err := h.resourceService.ListResources(c.Request.Context(), gvk, namespace)
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
	gvk := h.getGVKFromParams(c)
	namespace := c.Param("namespace")
	name := c.Param("name")

	err := h.resourceService.DeleteResource(c.Request.Context(), gvk, namespace, name)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Resource deleted successfully"})
}

// getGVKFromParams extracts GroupVersionKind from URL parameters
func (h *ResourceHandler) getGVKFromParams(c *gin.Context) schema.GroupVersionKind {
	group := c.Param("group")
	version := c.Param("version")
	kind := c.Param("kind")

	return schema.GroupVersionKind{
		Group:   group,
		Version: version,
		Kind:    kind,
	}
}
