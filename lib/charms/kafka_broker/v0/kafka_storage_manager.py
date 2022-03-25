"""

Implements a multi-entry storage management layer.

Kafka-related charms uses folders to store data in transit across the cluster.
This class implements a mechanism capable of formatting devices and mounting
folders to be used by the main charm.

It accepts managing storage both using juju storage features (i.e. it
implements observers for storage-related events) or accept to add/remove
storage devices based on options.


# Interface Methods

Implements the following methods as interfaces to manage storage:

    manage_volumes(self, volume_list)
    add_volume(self, volume_list)
    del_volume(self, volume_list)
    lst_volumes(self)
    lst_folders(self)
    set_default_user(self, user, mandatory=False)
    set_default_group(self, group, mandatory=False)
    set_default_group(self, fs)

manage_volumes
volume_list: compare this list with the one stored in the StorageManager. 
It will remove or add volumes that differs between stored and volume_list

add_volume
volume_list: should be a list of dicts containing:
[
    {
        "fs_path": <PATH>,
        "device": {
            "name": <DEVICE_PATH>,
            "filesystem": <OPTIONAL, FS_MODEL>,
            "options": <OPTIONAL, FS_MOUNT_OPTIONS>
        }, ## OPTIONAL
        "user": <OPTIONAL, username to create the path>,
        "group": <OPTIONAL, group name to create the path>
    },
    ...
]

del_volume
volume_list: list of dicts containing at least one of the following values 
in the dict:
[
    {
        "fs_path": <OPTIONAL, PATH>,
        "device": <OPTIONAL, DEVICE_PATH>,
    },
    ...
]

lst_volumes
returns the internal store's content of the mounted volumes, including
those mounted using juju storage.

lst_folders
returns a list of strings, each element is a volume

set_default_user,set_default_group
Set a default user/group to be used by add_volume. add_volume may define an
user/group on the method call but it can be overruled if "mandatory" is set
on the set_default_*

## Juju Events

Also implements an observe for the following Juju events:
    storage_attached
    storage_detaching


# Internal Data Model

That model is stored in the internal class store.

## Device Data

This section describes the model of the internal Store:

[
    {
        "fs_path": <PATH>,
        "device": {  
            "name": <DEVICE_PATH>,
            "filesystem": <FS_MODEL>,
            "options": <FS_MOUNT_OPTIONS>,
        }, ## OPTIONAL
        "user": <username to create the path>,
        "group": <group name to create the path>,
        "juju_managed": <TRUE/FALSE depending if it was mounted by Juju>,
        "juju_id": <ID string from juju, empty otherwise>
    },
    ...
]

## Internal config data

And the config data, which is a single dict:
{
    "mandatory_user": <empty, means no mandatory user>,
    "mandatory_group": <empty, means no mandatory group>,
    "default_user": "",
    "default_group": "",
    "default_fs": "xfs"
}


# How to use:

A charm willing to use this class can either implement juju storage as blocks.
On metadata.yaml:
    storage:
      <NAME>:
        type: block
        multiple:
          range: 0-
        minimum-size: 1G

And then, pass the storage <NAME> on the constructor of StorageManager:

    StorageManager(storage_name="<NAME>")

Or alternatively, the charm can load a config from its config.yaml and build the
dicts described above to use add_/del_/... volume methods.

"""

import os
import shutil
import subprocess
import uuid

from ops.framework import StoredState, Object

from charms.kafka_broker.v0.kafka_linux import userAdd, LinuxUserAlreadyExistsError

from charms.kafka_broker.v0.charmhelper import mount, umount


class StorageManagerError(Exception):
    """Report errors related to StorageManager class"""

    def __init__(self, msg):
        """Initializes method by passing exception msg onwards."""
        super().__init__(msg)


class StorageManager(Object):
    """The Storage Manager layer for both juju-related and config."""

    sm = StoredState()

    def __init__(self, charm, storage_name=""):
        """Initializes the storage manager"""
        super().__init__(charm, 'storage_manager')
        self.sm.set_default(data=[])
        self.sm.set_default(
            config={
                "mandatory_user": "",
                "mandatory_group": "",
                "default_user": "",
                "default_group": "",
                "default_fs": "xfs",
            }
        )
        if len(storage_name) > 0:
            self.framework.observe(
                charm.on[storage_name].storage_attached, self.on_storage_attached
            )
            self.framework.observe(
                charm.on[storage_name].storage_detaching, self.on_storage_detaching
            )

    def on_storage_attached(self, event):
        """Processes storage event if available."""
        # Generate a random path to be used
        path = "/data_{}".format(uuid.uuid4())
        self.add_volume(
            [
                {
                    "fs_path": path,
                    "device": {
                        "name": str(event.storage.location),
                        "filesystem": self.sm.config["default_fs"],
                    },
                }
            ],
            is_juju_managed=True,
            juju_id=event.storage.id,
        )

    def on_storage_detaching(self, event):
        """Detaches the storage using juju."""
        path = None
        for s in self.sm.data:
            if event.storage.id == s["juju_id"]:
                path = s["fs_path"]
                break
        self.del_volume([{"fs_path": path}])

    def manage_volumes(self, volume_list):
        """Remove any volumes not present in the volume_list but present in
        self.sm.data; adds volumes present in the volume_list and not in
        self.sm.data.
        """
        vol_lst = [v["fs_path"] for v in volume_list]
        self.del_volume(
            [{"fs_path": p["fs_path"]} for p in self.sm.data if p["fs_path"] not in vol_lst]
        )
        dat_lst = [v["fs_path"] for v in self.sm.data]
        self.add_volume(
            [p for p in volume_list if p["fs_path"] not in dat_lst]
        )

    def add_volume(self, volume_list, is_juju_managed=False, juju_id=""):
        """Add a new volume if not existing already."""
        for v in volume_list:
            if not self._validate_volume_schema(v):
                raise StorageManagerError("Wrong volume schema.")
            if v["fs_path"] in self.sm.data:
                raise StorageManagerError("Folder already added.")
            self._create_volume(v)
            new_vol = {**v, "juju_managed": is_juju_managed, "juju_id": juju_id}
            self.sm.data.append(new_vol)

    def del_volume(self, volume_list):
        """Removes the volume from management."""
        found = False
        for v in volume_list:
            path = None
            elem = None
            for p in self.sm.data:
                if (
                    "device" in v
                    and "device" in p
                    and v["device"] != p["device"]["name"]
                ) or ("fs_path" in v and v["fs_path"] == p["fs_path"]):
                    path = v["fs_path"]
                    found = True
                    elem = p
                    break
            if not found:
                raise StorageManagerError("Did not find volume {}".format(v))
            if umount(path):
                self.sm.data.remove(elem)
            # Restart found for the next run
            found = False

    def lst_volumes(self):
        """Returns a list of strings with fs_path of each entry."""
        return [p["fs_path"] for p in self.sm.data]

    def set_default_user(self, user, mandatory=False):
        self.sm.config["default_user"] = user
        if mandatory:
            self.sm.config["mandatory_user"] = user

    def set_default_group(self, group, mandatory=False):
        self.sm.config["default_group"] = group
        if mandatory:
            self.sm.config["mandatory_group"] = group

    def _create_volume(self, vol):
        """Creates the volume according to the data passed."""
        # Find the user and group
        if "user" in vol:
            user = (
                vol["user"]
                if len(self.sm.config["mandatory_user"]) == 0
                else self.sm.config["mandatory_user"]
            )
        elif len(self.sm.config["default_user"]) > 0:
            # Set the default user if empty
            user = self.sm.config["default_user"]
        else:
            user = "ubuntu"
        if "group" in vol:
            group = (
                vol["group"]
                if len(self.sm.config["mandatory_group"]) == 0
                else self.sm.config["mandatory_group"]
            )
        elif len(self.sm.config["default_group"]) > 0:
            # Set the default group if empty
            group = self.sm.config["default_group"]
        else:
            group = "ubuntu"
        # Create user + homedir if missing
        try:
            home = "/home/{}".format(user)
            userAdd(user, home=home, group=group)
            os.makedirs(home, 0o755, exist_ok=True)
            shutil.chown(home, user=user, group=group)
        except LinuxUserAlreadyExistsError:
            # This is only to check if the user already exists, if so, there
            # is nothing else to do, move along
            pass
        # Get the filesystem
        # Create the folder
        # Let it fail if folder already exists
        os.makedirs(vol["fs_path"], 0o750, exist_ok=True)
        shutil.chown(vol["fs_path"], user=user, group=group)
        # If the device is present, then mount it
        if "device" in vol:
            d = vol["device"]
            fs = d["filesystem"] if "filesystem" in d else self.sm.config["default_fs"]
            cmd = ["mkfs", "-t", fs, d["name"]]
            subprocess.check_call(cmd)
            mount(
                d["name"],
                vol["fs_path"],
                options=d.get("options", None),
                persist=True,
                filesystem=fs,
            )

    def _validate_volume_schema(self, vol):
        """Returns True if the volume (a dict) fits in the schema defined above.

        Following rules are checked:
            1) fs_path is mandatory string
            2) "device" is optional, if added, should contain the following fields:
                - name: <DEVICE_PATH>
                - filesystem: <FS_MODEL>
                - options: <FS_MOUNT_OPTIONS>

        Returns: True or False if fits the schema
        """
        if "fs_path" not in vol:
            return False
        # Device may not be requested for an specific entry
        if "device" in vol and "name" not in vol["device"]:
            return False
        return True
