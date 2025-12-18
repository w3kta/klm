We have inputs those are stored in the data folder:
./data/bookings/booking.json
./data/airports/airports.dat

## Features
- Lazy Evaluation

## Run
docker run -it KLM /bin/bash
OR
docker run -it -v "$(pwd):/app" klm
