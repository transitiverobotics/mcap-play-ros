#!/usr/bin/env python

import sys
import os
import time

import rospy
from genpy.dynamic import generate_dynamic

from mcap.reader import make_reader
from mcap_ros1.decoder import Decoder

import threading
from keylisten import listen

rospy.init_node('mcapPlayback', anonymous=True)

usage = """
During playback you can press:
 SPACE: toggle pause
 >: skip forward
 q: quit
"""

if len(sys.argv) > 2:
    speed = float(sys.argv[2])
else:
    speed = 1

publishers = {}
clock = -1;

paused = False
unpauseEvent = False
decoder = Decoder()

def publish(topic, schema, message):
    global clock
    global paused
    global unpauseEvent

    # If we have not yet seen this topic before, generate message class on-the-fly
    # from the included schema.data and create a new publisher for the topic.
    if not topic in publishers:
        decoded = schema.data.decode()
        msgs = generate_dynamic(schema.name, decoded)
        pub = rospy.Publisher(topic, msgs[schema.name], queue_size=10)
        publishers[topic] = pub

    rosMessage = decoder.decode(schema, message)
    # wait until it is time to publish this message
    if clock > 0 and message.publish_time > clock:
        time.sleep((message.publish_time - clock) / 1e9 / speed)
    clock = max(clock, message.publish_time)

    if paused:
      unpauseEvent.wait()

    print(time.ctime(clock / 1e9), end='\r')
    publishers[topic].publish(rosMessage)

# -------------------------------------------------------------
# handle keyboard control input

def on_press(key):
  """
  Implements basic keyboard controls to toggle pause, and maybe other things
  """
  global paused
  global clock
  global unpauseEvent

  if key == ' ':
    paused = not paused
    print('paused:', paused)
    if paused:
      unpauseEvent = threading.Event()
    else:
      if unpauseEvent:
        unpauseEvent.set()
  elif key == '>':
    clock += speed * 10 * 1e9
  elif key == 'q':
    print('quit')
    os._exit(0)
  else:
    print('unknown key', repr(key))

# -----------------------

def main():
  with open(sys.argv[1], "rb") as f:
      reader = make_reader(f)
      for schema, channel, message in reader.iter_messages():
          if rospy.is_shutdown(): exit()
          if channel.message_encoding == 'ros1':
              publish(channel.topic, schema, message)


if __name__ == '__main__':
  print(usage)
  threads = []
  threads.append(threading.Thread(target=listen, args=[on_press]))
  threads.append(threading.Thread(target=main))
  for thread in threads:
      thread.start()
  for thread in threads:
      thread.join()
      os._exit(0)

