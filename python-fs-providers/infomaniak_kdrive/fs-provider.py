from dataiku.fsprovider import FSProvider
from infomaniak_client import KdriveClient, Item
import os, shutil
from io import BytesIO


class CustomFSProvider(FSProvider):
    def __init__(self, root, config, plugin_config):
        """
        :param root: the root path for this provider
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        if len(root) > 0 and root[0] == '/':
            root = root[1:]
        self.root = root
        self.provider_root = "/"
        root_url = config.get("root_url")

        auth = config.get("api_token", {})
        api_token = auth.get("api_token")
        self.client = KdriveClient(api_token=api_token)
        self.drive_id, self.root_file_id = extract_id_from_url(root_url)


    # util methods
    def get_rel_path(self, path):
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        return path

    def get_lnt_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)

    def get_full_path(self, path):
        path_elts = [self.provider_root, self.get_rel_path(self.root), self.get_rel_path(path)]
        path_elts = [e for e in path_elts if len(e) > 0]
        return os.path.join(*path_elts)

    def close(self):
        """
        Perform any necessary cleanup
        """
        print('close')

    def stat(self, path):
        """
        Get the info about the object at the given path inside the provider's root, or None 
        if the object doesn't exist
        """
        full_path = self.get_full_path(path)
        print("stat:full_path={}".format(full_path)) # :stat:full_path=/first/second/outside.png
        # why :stat:full_path=/first/interpolated2.png/interpolated2.png ?
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            return None
        item_description = item.get_description()
        if item.is_folder():
            ret = {
                "path": self.get_lnt_path(path),
                "size": 0,
                "lastModified": item_description.get("lastModified"),
                "isDirectory": True
            }
            return ret
        else:
            ret = {
                "path": self.get_lnt_path(path),
                "size": item_description.get("size"),
                "lastModified": item_description.get("lastModified"),
                "isDirectory": False
            }
            return ret
        # {
        #     "name": self.descriptor.get("name"),
        #     "lastModified": int(self.descriptor.get("last_modified_at")) * 1000,
        #     "size": self.get_size(),
        #     "fullPath": "/".join([self.path, self.descriptor.get("name")]),
        #     "directory": self.is_folder()
        # }
        # if not os.path.exists(full_path):
        #     return None
        # if os.path.isdir(full_path):
        #     return {'path': self.get_lnt_path(path), 'size':0, 'lastModified':int(os.path.getmtime(full_path)) * 1000, 'isDirectory':True}
        # else:
        #     return {'path': self.get_lnt_path(path), 'size':os.path.getsize(full_path), 'lastModified':int(os.path.getmtime(full_path)) * 1000, 'isDirectory':False}

    def set_last_modified(self, path, last_modified):
        """
        Set the modification time on the object denoted by path. Return False if not possible
        """
        full_path = self.get_full_path(path)
        os.utime(full_path, (os.path.getatime(full_path), last_modified / 1000))
        return True

    def browse(self, path):
        """
        List the file or directory at the given path, and its children (if directory)
        """
        full_path = self.get_full_path(path)
        print("browse:full_path={}, self.drive_id={}, self.root_file_id={}".format(full_path, self.drive_id, self.root_file_id))
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            ret = {'fullPath' : None, 'exists' : False}
            return ret
        elif item.is_file():
            ret = item.get_description()
            return ret
        else:
            children = []
            for child in item.get_next_child():
                children.append(
                    child
                )
            ret = {
                'fullPath' : self.get_lnt_path(path),
                'exists' : True,
                'directory' : True,
                'children' : children
            }
            return ret

    def enumerate(self, path, first_non_empty):
        """
        Enumerate files recursively from prefix. If first_non_empty, stop at the first non-empty file.

        If the prefix doesn't denote a file or folder, return None
        """
        full_path = self.get_full_path(path)
        print("enumerate:full_path={}".format(full_path))
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            return None
        if item.is_file():
            item_description = item.get_description()
            ret = [
                {
                    'path': self.get_lnt_path(path),
                    'size': item_description.get("size"),
                    'lastModified': item_description.get("lastModified")
                }
            ]
            return ret
        ret = self.list_recursive(item, path, full_path, first_non_empty)
        return ret

    def list_recursive(self, folder_item, path, full_path, first_non_empty):
        paths = []
        print("list_recursive={}/{}".format(path, full_path))
        for child in folder_item.get_next_child():
            if child.get("directory"):
                new_paths = self.list_recursive(
                    Item(self.client, self.drive_id, path, child),
                    self.get_lnt_path(os.path.join(path, child.get("name"))),
                    self.get_lnt_path(os.path.join(full_path, child.get("name"))),
                    first_non_empty
                )
                paths.extend(
                    new_paths
                )
            else:
                full_sub_path = os.path.join(path, child.get("name"))
                sub_path = full_sub_path[len(os.path.join(self.provider_root, self.root)):]
                paths.append(
                    {
                        'path': self.get_lnt_path(full_sub_path),
                        'size': child.get("size"),
                        'lastModified': child.get("lastModified")
                    }
                )
            # {
            #     "name": item.get("name"),
            #     "lastModified": int(item.get("last_modified_at")) * 1000,
            #     "size": self.get_size(item=item),
            #     "fullPath": "/".join([self.path, item.get("name")]),
            #     "directory": item.get("type")=="dir"
            # }
        return paths

    def delete_recursive(self, path):
        """
        Delete recursively from path. Return the number of deleted files (optional)
        """
        full_path = self.get_full_path(path)
        print("delete_recursive:full_path={}".format(full_path)) # :delete_recursive:full_path=/first/Screenshot 2025-03-06 at 16.07.27.png
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            return 0
        else:
            item.delete()
            return 1
        # if not os.path.exists(full_path):
        #     return 0
        # elif os.path.isfile(full_path):
        #     os.remove(full_path)
        #     return 1
        # else:
        #     shutil.rmtree(full_path)
        #     return 0

    def move(self, from_path, to_path):
        """
        Move a file or folder to a new path inside the provider's root. Return false if the moved file didn't exist
        """
        full_from_path = self.get_full_path(from_path)
        full_to_path = self.get_full_path(to_path)
        if are_files_in_same_path(full_from_path, full_to_path):
            # simple rename
            _, file_name = os.path.split(full_to_path)
            item_to_rename = self.client.get_item(self.drive_id, self.root_file_id, full_from_path.strip("/"), self.get_lnt_path(full_from_path).strip("/"))
            if not item_to_rename.exists():
                return False
            self.client.rename(self.drive_id, item_to_rename.get_file_id(), file_name)
            return True
        else:
            # move into new dir
            item_from = self.client.get_item(self.drive_id, self.root_file_id, full_from_path.strip("/"), self.get_lnt_path(full_from_path).strip("/"))
            destination_path, _ = os.path.split(full_to_path)
            item_to = self.client.get_item(self.drive_id, self.root_file_id, destination_path.strip("/"), self.get_lnt_path(destination_path).strip("/"))
            self.client.move_item(self.drive_id, item_from.get_file_id(), item_to.get_file_id())
            return True

    def read(self, path, stream, limit):
        """
        Read the object denoted by path into the stream. Limit is an optional bound on the number of bytes to send
        """
        full_path = self.get_full_path(path)
        print("read:full_path={}".format(full_path)) # :read:full_path=/first/second/outside.png
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path.strip("/"), self.get_lnt_path(path).strip("/"))
        if not item.exists():
            raise Exception('Path doesn t exist')
        response = self.client.get_file_content(self.drive_id, item.get_file_id())
        bio = BytesIO(response.content)
        shutil.copyfileobj(bio, stream)

    def write(self, path, stream):
        """
        Write the stream to the object denoted by path into the stream
        """
        full_path = self.get_full_path(path)
        print("write:full_path={}".format(full_path))
        full_path_parent = os.path.dirname(full_path)
        item = self.client.get_item(self.drive_id, self.root_file_id, full_path_parent.strip("/"), self.get_lnt_path(full_path_parent).strip("/"), create_folder=True)
        parent_folder_id = item.get_file_id()
        #if not item.exists():
        #    print("doesn't exists, creating")
        #    parent_folder_id = self.client.make_dirs(self.drive_id, self.root_file_id, full_path_parent)
        bio = BytesIO()
        shutil.copyfileobj(stream, bio)
        bio.seek(0)
        data = bio.read()
        response = self.client.write_file_content(self.drive_id, parent_folder_id, full_path, data)


def extract_id_from_url(url):
    if not url:
        return None, None
    tokens = url.strip('/').split('/')
    drive_id = file_id = None
    if tokens[-2:-1][0] == "files" and tokens[-4:-3][0]=="drive":
        drive_id = tokens[-3:-2][0]
        file_id = tokens[-1:][0]
    return drive_id, file_id

def are_files_in_same_path(path_one, path_two):
    file_path_one, _ = os.path.split(path_one)
    file_path_two, _ = os.path.split(path_two)
    return file_path_one == file_path_two
