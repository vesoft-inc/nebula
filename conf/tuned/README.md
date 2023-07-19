# Summary

These are tuned profile to configure the system to optimize for the NebulaGraph service.

Follow below steps to utilize:
 * Install the tuned service if absent, and enable it with `systemctl`.
 * Copy the __nebula__ directory into `/etc/tuned`.
 * Execute `tuned-adm profile nebula` to activate the profile.
