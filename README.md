check_sidekiq is a Nagios plugin that checks the consumption rate of Sidekiq queues, written in python because irony
All the configured queues in a given server are checked
Settings for each queue may be overridden on the command line

Methodology
-----------
    check the size of the queue
    if the queue size is under a "noise" threshold return OK
        this will miss the case where the queue has just started to grow, but we'll catch it on the next check
    if the queue size is over the noise threshold wait a certain amount of time
    check the size of the queue again
    the difference between the two checks shows the rate of processing
    if the rate of processing is negative or zero, that's a CRITICAL
    if the rate of processing is positive but slow that's a WARNING
    if the rate of processing is acceptably fast that's an OK

Usage
-----

check_sidekiq.py *URL* [--`queue`-noise *noise floor* ...] [--`queue`-crit *critical processing rate* ...] [--`queue`-warn *warning processing rate* ...] [--sleep *time between checks*]

Defaults
--------
noise 5

crit 0

warn 5

sleep 30
