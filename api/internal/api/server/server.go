package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/tsamsiyu/themelio/api/internal/api/handlers"
	"github.com/tsamsiyu/themelio/api/internal/api/middleware"
	"github.com/tsamsiyu/themelio/api/internal/config"
)

type Server struct {
	config *config.ServerConfig
	logger *zap.Logger
	router *gin.Engine
	server *http.Server
}

func NewRouter(logger *zap.Logger, resourceHandler *handlers.ResourceHandler, watchHandler *handlers.WatchHandler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	router.Use(middleware.ErrorHandler(logger))
	router.Use(middleware.ErrorMapper(logger))
	router.Use(middleware.RequestLogger(logger))
	router.Use(middleware.CORSMiddleware())

	api := router.Group("/api/v1")
	{
		resources := api.Group("/resources")
		{
			resources.PUT("/:group/:version/:kind", resourceHandler.ReplaceResource)
			resources.GET("/:group/:version/:kind/:name", resourceHandler.GetResource)
			resources.GET("/:group/:version/:kind", resourceHandler.ListResources)
			resources.DELETE("/:group/:version/:kind/:name", resourceHandler.DeleteResource)
			resources.PATCH("/:group/:version/:kind/:name", resourceHandler.PatchResource)
			resources.GET("/:group/:version/:kind/watch", watchHandler.WatchResource)
		}
	}

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	return router
}

func NewServer(
	cfg *config.Config,
	logger *zap.Logger,
	router *gin.Engine,
) *Server {
	return &Server{
		config: &cfg.Server,
		logger: logger,
		router: router,
	}
}

func (s *Server) Start(ctx context.Context) error {
	s.server = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", s.config.Host, s.config.Port),
		Handler:      s.router,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
	}

	s.logger.Info("Starting HTTP server",
		zap.String("addr", s.server.Addr),
		zap.Int("port", s.config.Port))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server failed", zap.Error(err))
		}
	}()

	<-ctx.Done()
	return s.Stop(ctx)
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping HTTP server")

	shutdownCtx, cancel := context.WithTimeout(ctx, s.config.ShutdownTimeout)
	defer cancel()

	return s.server.Shutdown(shutdownCtx)
}
