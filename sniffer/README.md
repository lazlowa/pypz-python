# Overview

This is an experimental tool be able to sniff the control information of the channels between the
operators.
This sniffer is only capable to sniff kafka based channels i.e., if 2 or more operators are 
connected via KafkaInput/OutputChannels (evtl. plugins).
The sniffer uses tkInter for visualization.

# Building into exe

The sniffer can be built into an executable. The package pyinstaller was tested, hence it is proposed
to be used for this purpose. You can install the pyinstaller via pip.
Once it is installed, you shall execute the following command:
```console pyinstaller -F path/to/SnifferRunnable.py```
or you can navigate to the folder containing the script and simply execute:
```console pyinstaller -F SnifferRunnable.py```
The final executable will be placed into ./dist folder in the execution's location.