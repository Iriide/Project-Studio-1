from tkinter import messagebox
import geocoder
import tkinter as tk
from tkinter import ttk
import pickle
import folium
import numpy as np
import requests
from selenium import webdriver
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep

latitude = None
longitude = None
power = None
model = pickle.load(open('model.pkl', 'rb'))
driver = webdriver.Chrome(ChromeDriverManager().install())


def get_current_location():
    """
    Get the current geographical location based on the IP address and update the map image.
    Returns the latitude, longitude, and address if successful; otherwise, returns None.
    """
    global latitude, longitude
    current_location = geocoder.ip("me")
    if current_location.latlng:
        latitude, longitude = current_location.latlng
        update_map_image((latitude, longitude))
        address = current_location.address
        return latitude, longitude, address
    else:
        return None


def get_coordinates_from_address(address):
    """
    Get geographical coordinates (latitude, longitude) for a given address.
    Returns the coordinates if successful; otherwise, returns None.
    """
    location = geocoder.osm(address)
    if location.latlng:
        return location.latlng
    else:
        return None


def check_current_location():
    """
    Check and display the current location. Updates the map and result display with the current location details.
    Shows an error message if the location cannot be retrieved.
    """
    global latitude, longitude
    current_location = get_current_location()
    if current_location:
        update_map_image((latitude, longitude))
        latitude, longitude, address = current_location
        result.set(f"Coordinates:\nLatitude: {latitude}\nLongitude: {longitude}")
    else:
        messagebox.showerror("Error", "Failed to retrieve current location")


def input_address():
    """
    Retrieve and display geographical coordinates based on the user-input address.
    Updates the map and result display with the new location details.
    Shows an error message if the coordinates cannot be retrieved.
    """
    global latitude, longitude

    input_address = address_entry.get()
    coordinates = get_coordinates_from_address(input_address)
    if coordinates:
        latitude, longitude = coordinates
        update_map_image((latitude, longitude))
        result.set(f"Coordinates for the address:\nLatitude: {latitude}\nLongitude: {longitude}")
    else:
        messagebox.showerror("Error", "Failed to retrieve coordinates for the address")


def calculate():
    """
    Calculate and display the predicted energy production based on the current location and power input.
    Uses a machine learning model for prediction. Displays an error message if the required data is not available.
    """
    global latitude, longitude
    power = power_entry.get()

    if latitude is not None and longitude is not None and power is not None:
        # API URL construction with latitude and longitude for weather data retrieval.
        url = f"https://re.jrc.ec.europa.eu/api/v5_2/MRcalc?lat={latitude}&lon={longitude}&outputformat=json&startyear=2020&endyear=2020&avtemp=1&horirrad=1"

        # API request for weather data.
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            H_h_m = []  # Solar irradiance.
            T2m = []  # Temperature.
            for i in range(12):
                H_h_m.append(data['outputs']['monthly'][i]['H(h)_m'])
                T2m.append(data['outputs']['monthly'][i]['T2m'])
        else:
            messagebox.showerror("Error", "Failed to retrieve data from API")
    else:
        messagebox.showwarning("Warning", "No coordinates available for API request")
    production_data = []
    for i in range(12):
        # Prepare data for the model prediction.
        array = np.array([i + 1, power, longitude, latitude, H_h_m[i], T2m[i]]).reshape(1, -1).astype(np.float64)
        production_data.append(int(model.predict(array)))

    # Display the predicted energy production.
    chart = create_chart(production_data)

    # Wyświetlanie wykresu w Tkinter
    chart_canvas = FigureCanvasTkAgg(chart, master=chart_frame)
    chart_canvas_widget = chart_canvas.get_tk_widget()
    chart_canvas_widget.pack(expand=True, fill='both')
    result.set(f"Predicted energy production: {sum(production_data)} kWh")
    return production_data

def create_chart(production_data):
    months = ['Sty', 'Lut', 'Mar', 'Kwi', 'Maj', 'Cze', 'Lip', 'Sie', 'Wrz', 'Paź', 'Lis', 'Gru']
    px = 1 / plt.rcParams['figure.dpi'] # pixel in inches
    fig, ax = plt.subplots(figsize=(400*px, 350*px))
    fig.subplots_adjust(left=0.2, bottom=0.2, right=0.95, top=0.9, wspace=0.2, hspace=0.2)
    ax.plot(months, production_data)
    ax.set_xlabel('Miesiące')
    ax.set_ylabel('Produkcja energii (kWh)')
    ax.set_title('Roczna Produkcja Energii')
    return fig

def update_map_image(coordinates, output_image_path='map_image.png'):
    """
    Update the map display with a new location. Generates a map image using Folium and Html2Image.
    """
    from pathlib import Path
    html_file = Path.cwd() / 'mapa_polski.html'
    global map_display


    # Create a Folium map centered at the given coordinates.
    mapa = folium.Map(location=coordinates, zoom_start=5)  # Set initial location and zoom for the map.

    # Add a marker to the map at the specified coordinates.
    folium.CircleMarker(location=coordinates).add_to(mapa)

    # Save the map to an HTML file.
    mapa.save("mapa_polski.html")
    sleep(0.5)  # Wait for the file to be saved properly.
    driver.get(html_file.as_uri())
    driver.set_window_size(300, 500)
    sleep(0.5)  # Wait for the map to be loaded properly.
    # Capture a screenshot of the HTML map and save it as an image.
    driver.save_screenshot(output_image_path)
    # Update the Tkinter map display with the new image.
    global map_image
    map_image = tk.PhotoImage(file=output_image_path)
    map_display.config(image=map_image)
    map_display.image = map_image  # Keep a reference to avoid garbage collection.

root = tk.Tk()

# Initialize the main window
root.title("Model predykcyjny produkcji energii elektrycznej z instalacji fotowoltaicznych w Polsce w zależności od lokalizacji i warunków pogodowych")
root.configure()

# Main frame
main_frame = ttk.Frame(root, padding="10", style='TFrame')
main_frame.pack(expand=True, fill="both")

# Address entry with label
address_label = ttk.Label(main_frame, text="Enter Address:")
address_label.pack(fill='x', expand=True)
address_entry = ttk.Entry(main_frame)
address_entry.pack(fill='x', expand=True, pady=5)

# Power entry with label
power_label = ttk.Label(main_frame, text="Enter Power (kW):")
power_label.pack(fill='x', expand=True)
power_entry = ttk.Entry(main_frame)
power_entry.pack(fill='x', expand=True, pady=5)

# Buttons
buttons_frame = ttk.Frame(main_frame, padding="5")
buttons_frame.pack(fill='x', expand=True, pady=5)

# 'Check Current Location' button
check_location_button = ttk.Button(buttons_frame, text="Check Current Location", command=check_current_location)
check_location_button.pack(side='left', expand=True, fill='x', padx=5)

# 'Get Coordinates' button
get_coordinates_button = ttk.Button(buttons_frame, text="Get Coordinates", command=input_address)
get_coordinates_button.pack(side='right', expand=True, fill='x', padx=5)

# Calculate button
calculate_button = ttk.Button(main_frame, text="Calculate", command=calculate)
calculate_button.pack(fill='x', expand=True, pady=5)

# Map image display
map_image = tk.PhotoImage(file="map_image.png")  # Placeholder for the actual path to the map image
map_display = ttk.Label(main_frame, image=map_image)
map_display.pack(fill='x', expand=True, pady=5)
update_map_image((52.237049, 21.017532))  # Ensure this function exists and works correctly

# Chart display
chart_frame = ttk.Frame(main_frame, padding="5")
chart_frame.pack(fill='both', expand=True, pady=5)

# Result display with label
result = tk.StringVar()
result_display = ttk.Label(main_frame, textvariable=result, background="white", relief="sunken")
result_display.pack(fill='x', expand=True, pady=5)

root.minsize(300, 200)

# Start the GUI event loop
root.mainloop()
