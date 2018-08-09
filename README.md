# Top of the Pile
### Prerequisites
- Python 3
- Docker
### Install
```
pip3 install -r requirements.txt
```
Edit ```config.ini``` with your indeed publisher number. If you do not have a publisher number, you can receive one by heading to the [Indeed Publisher Portal](http://www.indeed.com/publisher).
```
[INDEED]
PublisherNumber = <Indeed_Publisher_Numer_Here>
```
### Usage
Run a mongo and mail server instances
```
docker-compose up
```
Run a task
```
python3 top_of_the_pile.py <Task> (--locations locations) (--verbose)

```
#### Example Tasks
For example, to monitor new jobs in San Fransisco, San Jose, and Moution View, you would do the following:
```
python3 top_of_the_pile.py monitor_indeed --locations "San Fransisco, California" "San Jose, California" "Moution View, California"

```
