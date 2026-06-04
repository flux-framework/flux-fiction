from collections.abc import Iterator
import logging
import heapq

logger = logging.getLogger(__name__)

class EventList(Iterator):
    '''
    Class that is used to store all events that happen within the emulator along with the time that they will occur

    For example: the submit time for each job is added to the event list at the initialization of the emulator.

    The internal loop of the emulator will handle all events that occur at the same time. Then, it waits for some set of conditions
    to occur before executing the next set of events
    '''
    def __init__(self):
        self.time_heap = []
        self.time_map = {}    
        self._current_time = None

    def add_event(self, time, callback):
        '''
        Add an event to the event list

        Takes in a time that the event will occur and a callback function to be invoked at that time
        '''
        if self._current_time is not None and time <= self._current_time:
            logger.warning(
                "Adding a new event at a time ({}) <= the current time ({})".format(
                    time, self._current_time
                )
            )

        if time in self.time_map:
            self.time_map[time].append(callback)
        else:
            new_event_list = [callback]
            self.time_map[time] = new_event_list
            heapq.heappush(self.time_heap, (time, new_event_list))

    def __len__(self):
        return len(self.time_heap)

    def __iter__(self):
        return self

    def min(self):
        return self.time_heap[0] if self.time_heap else None

    def max(self):
        if not self.time_heap:
            return None
        time = max(self.time_map.keys())
        return self.time_map[time]

    def __next__(self):
        try:
            time, event_list = heapq.heappop(self.time_heap)
            self.time_map.pop(time)
            self._current_time = time  
            return time, event_list
        except (IndexError, KeyError):
            raise StopIteration()