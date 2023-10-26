package imageutils

import (
	"fmt"
	"image"
	"net/http"
	"os"
	"path/filepath"

	"github.com/disintegration/imaging"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

func CompressImage(url string) (string, error) {
	// Make GET request to URL
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Check if response status code is 200 OK
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}

	// Decode response body into image
	img, _, err := image.Decode(resp.Body)
	if err != nil {
		return "", err
	}

	// Create new image with smaller size
	newImg := imaging.Resize(img, 100, 0, imaging.Lanczos)

	// Create new file with unique name in local directory
	filename := uuid.New().String()
	newFilename := fmt.Sprintf("%s_compressed%s", &filename, ".jpg")
	filepath := filepath.Join(viper.GetString("files.basePath"), newFilename)
	file, err := os.Create(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Write compressed image to file
	err = imaging.Encode(file, newImg, imaging.JPEG)
	if err != nil {
		return "", err
	}

	return filepath, nil
}
