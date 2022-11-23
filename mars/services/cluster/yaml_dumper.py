import os
import yaml
import tempfile

from ... import oscar as mo


class YamlDumperActor(mo.Actor):

    def __init__(self):
        self.yaml_root_dir = os.path.join(tempfile.tempdir, "mars_temp_yaml")

    async def save_yaml(self, obj, save_path):
        abs_save_path = os.path.join(self.yaml_root_dir, save_path)
        abs_save_dir, _ = os.path.split(abs_save_path)
        if not os.path.exists(abs_save_dir):
            os.makedirs(abs_save_dir)

        if os.path.isfile(abs_save_path):
            mode = "a"
        else:
            mode = "w"
        with open(abs_save_path, mode) as f:
            yaml.dump(obj, f)
