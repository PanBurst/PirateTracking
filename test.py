import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from cartopy.io import img_tiles

# Create a map with satellite imagery
plt.figure(figsize=(10, 5))
ax = plt.axes(projection=ccrs.PlateCarree())

# Add satellite imagery (Google Tiles)
google_tiles = img_tiles.GoogleTiles()
ax.add_image(google_tiles, 6)  # 6 is the zoom level, you can adjust it as needed

# Set the extent of the map
ax.set_extent([-125, -65, 20, 50], crs=ccrs.PlateCarree())

# Add coastlines
ax.coastlines()

# Plot a point
lat, lon = 37.7749, -122.4194  # San Francisco
plt.plot(lon, lat, 'ro', markersize=8, label='San Francisco')
plt.text(lon, lat, 'San Francisco', fontsize=10, va='bottom', ha='center')

# Set title and legend
plt.title('Map of San Francisco with Satellite Imagery')
plt.legend()

# Show the map
plt.show()
