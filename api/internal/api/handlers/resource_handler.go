package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/api/errors"
	servicetypes "github.com/tsamsiyu/themelio/api/internal/service/types"
)

type ResourceHandler struct {
	logger          *zap.Logger
	resourceService servicetypes.ResourceService
	validator       *validator.Validate
}

func NewResourceHandler(
	logger *zap.Logger,
	resourceService servicetypes.ResourceService,
	validator *validator.Validate,
) *ResourceHandler {
	return &ResourceHandler{
		logger:          logger,
		resourceService: resourceService,
		validator:       validator,
	}
}

func (h *ResourceHandler) ReplaceResource(c *gin.Context) {
	params, err := getParamsFromContext(c)
	if err != nil {
		c.Error(err)
		return
	}

	jsonData, err := c.GetRawData()
	if err != nil {
		c.Error(errors.NewSerializationError("reading request body", err))
		return
	}

	err = h.resourceService.ReplaceResource(c.Request.Context(), params, jsonData)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"OK": true})
}

func (h *ResourceHandler) GetResource(c *gin.Context) {
	params, err := getParamsFromContext(c)
	if err != nil {
		c.Error(err)
		return
	}

	resource, err := h.resourceService.GetResource(c.Request.Context(), params)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, resource)
}

func (h *ResourceHandler) ListResources(c *gin.Context) {
	params, err := getParamsFromContext(c)
	if err != nil {
		c.Error(err)
		return
	}

	resources, err := h.resourceService.ListResources(c.Request.Context(), params)
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
	params, err := getParamsFromContext(c)
	if err != nil {
		c.Error(err)
		return
	}

	err = h.resourceService.DeleteResource(c.Request.Context(), params)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Resource deleted successfully"})
}

func (h *ResourceHandler) PatchResource(c *gin.Context) {
	params, err := getParamsFromContext(c)
	if err != nil {
		c.Error(err)
		return
	}

	patchData, err := c.GetRawData()
	if err != nil {
		c.Error(errors.NewSerializationError("reading request body", err))
		return
	}

	patchedResource, err := h.resourceService.PatchResource(c.Request.Context(), params, patchData)
	if err != nil {
		c.Error(err)
		return
	}

	c.JSON(http.StatusOK, patchedResource)
}

func getParamsFromContext(c *gin.Context) (servicetypes.Params, error) {
	group := c.Param("group")
	version := c.Param("version")
	kind := c.Param("kind")
	namespace := c.Param("namespace")
	name := c.Param("name")

	return servicetypes.Params{
		Group:     group,
		Version:   version,
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
	}, nil
}
