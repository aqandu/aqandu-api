from enum import Enum
import common.utils
import string, secrets
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class FS_API_OBJ(object):
    def __init__(self, identifier=None, quota=None, key=None, used=None, new=False):
        self.identifier = identifier
        self.used = used
        self.new = new
        self.remaining = None
        self.quota = quota
        assert quota is None or quota >= -1

        if new and key is None:
            self.key = FS_ACCESS.keygen()
        else:
            self.key = key

        if self.new:
            self.isnew()

        self.update_remaining_quota()

    @classmethod
    def from_dict(cls, d, **kwargs):
        allowed = (f'{FS_ACCESS.IDENTIFIER}', f'{FS_ACCESS.QUOTA}', f'{FS_ACCESS.KEY}', f'{FS_ACCESS.QUOTA_USED}', f'new')
        params = {k.lower():v for k, v in d.items() if k in allowed}
        params.update(kwargs)
        return cls(**params)

    def to_dict(self):
        d = {
            f'{FS_ACCESS.KEY}': self.key,
            f'{FS_ACCESS.QUOTA}': self.quota,
            f'{FS_ACCESS.QUOTA_USED}': self.used
        }
        return {k:v for k,v in d.items() if v is not None}

    def isfull(self):
        return all(self.identifier, self.quota, self.key, self.used)
    
    def isnew(self):
        self.used = 0
        self.remaining = self.quota

    def update(self, api_obj):
        if api_obj.key is not None:
            self.key = api_obj.key 
        if api_obj.quota is not None:
            self.quota = api_obj.quota
        if api_obj.used is not None:
            self.used = api_obj.used
        if api_obj.identifier is not None:
            # Just a little check to make sure we're not combining objects
            assert api_obj.identifier == self.identifier
        self.update_remaining_quota()

    def update_remaining_quota(self):
        if self.quota is not None and self.used is not None:
            self.remaining = self.quota - self.used
        else:
            self.remaining = None
        print(f'quota remaining update. Remaining: {self.remaining}')

    def update_quota_values(self, just_used:int):
        self.used += just_used
        self.update_remaining_quota()


class FS_ACCESS(str, Enum):

    API = 'API'
    API_KEY = 'API.Key'
    QUOTA = 'Quota'
    IDENTIFIER = 'Identifier'
    QUOTA_USED = 'Used'
    KEY = 'Key'
    USERS_COL = 'users'

    @staticmethod
    def keygen(length=16):
        alphabet = string.ascii_uppercase + string.digits
        return ''.join(secrets.choice(alphabet) for i in range(length))

    @staticmethod
    def get_fs_client():
        if not hasattr(FS_ACCESS.get_fs_client, "client"):
            if not hasattr(FS_ACCESS.get_fs_client, "initialized"):
                FS_ACCESS.get_fs_client.initialized = True
                firebase_admin.initialize_app()
            FS_ACCESS.get_fs_client.client = firestore.client()
        return FS_ACCESS.get_fs_client.client

    @staticmethod 
    def get_fs_collection(col):
        db = FS_ACCESS.get_fs_client()
        if not hasattr(FS_ACCESS.get_fs_collection, "col_dict"):
            FS_ACCESS.get_fs_collection.col_dict = {}
        if col not in FS_ACCESS.get_fs_collection.col_dict.keys():
            FS_ACCESS.get_fs_collection.col_dict[col] = db.collection(col)
        return FS_ACCESS.get_fs_collection.col_dict[col]

    @staticmethod
    def identifier_exists(identifier:str) -> bool:
        col_ref = FS_ACCESS.get_fs_collection(FS_ACCESS.USERS_COL)
        doc_ref = col_ref.document(identifier)
        doc = doc_ref.get()
        return doc.exists

    @staticmethod
    def key_exists(key:str) -> bool:
        return bool(FS_ACCESS.get_api_obj_for_key(key))

    @staticmethod
    def update_api_obj(update_obj:FS_API_OBJ):
        """
        If the field is of type "map" then the entire field gets rewritten
        with the update object. So we have to pull the old map, combine it
        with the update k:v pairs, then upload that.
        """
        col = FS_ACCESS.get_fs_collection(FS_ACCESS.USERS_COL)
        stored_api_obj = FS_ACCESS.get_api_obj_for_identifier(update_obj.identifier)

        stored_api_obj.update(update_obj)

        doc_ref = col.document(stored_api_obj.identifier)
        doc_ref.update({f'{FS_ACCESS.API}': stored_api_obj.to_dict()})

    @staticmethod
    def create_api_obj(api_obj:FS_API_OBJ):
        db = FS_ACCESS.get_fs_client()
        doc_ref = db.collection(FS_ACCESS.USERS_COL).document(api_obj.identifier)
        doc_ref.set({
            f'{FS_ACCESS.IDENTIFIER}': api_obj.identifier,
            f'{FS_ACCESS.API}': api_obj.to_dict()
        })

    @staticmethod
    def get_api_obj_for_identifier(identifier:str) -> FS_API_OBJ:
        col = FS_ACCESS.get_fs_collection(FS_ACCESS.USERS_COL)
        doc_ref = col.document(identifier).get()
        try:
            api_obj = FS_API_OBJ.from_dict(doc_ref.to_dict()['API'], identifier=identifier)
            return api_obj
        except:
            return None

    @staticmethod
    def get_api_obj_for_key(key:str) -> FS_API_OBJ:
        col = FS_ACCESS.get_fs_collection(FS_ACCESS.USERS_COL)
        try:
            doc_ref = next(col.where(f"{FS_ACCESS.API_KEY}", "==", f'{key}').limit(1).stream())
            api_obj = FS_API_OBJ.from_dict(doc_ref.to_dict()['API'], identifier=doc_ref.id)
            return api_obj
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def get_quota_for_key(key:str) -> int:
        api_obj = FS_ACCESS.get_api_obj_for_key(key)
        try:
            return api_obj.quota
        except:
            return None

    @staticmethod
    def get_quota_used_for_key(key:str) -> int:
        api_obj = FS_ACCESS.get_api_obj_for_key(key)
        try:
            return api_obj.used
        except:
            return None

    @staticmethod
    def get_quota_remaining_for_key(key:str) -> int:
        api_obj = FS_ACCESS.get_api_obj_for_key(key)
        try:
            return api_obj.quota - api_obj.used
        except:
            return None