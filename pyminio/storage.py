import io
from typing import Optional, overload
from minio import Minio
from minio.helpers import ObjectWriteResult
from minio.datatypes import Object, Bucket
import concurrent.futures

from pyminio.utils.extensions import bytes_to_io


class Storage():
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self._current_bucket = None
    
    @property
    def client(self) -> Minio:
        """
        Get the Minio client.
        
        Returns:
            Minio
        """
        return Minio(self.endpoint, self.access_key, self.secret_key)
    
    @property
    def current_bucket(self) -> Optional[str]:
        """
        Get the current bucket.
        
        Returns:
            Optional[str]
        """
        return self._current_bucket
    
    @current_bucket.setter
    def current_bucket(self, value: str) -> None:
        """
        Set the current bucket.
        
        Args:
            value: The name of the bucket to set.
        
        Returns:
            None
        """
        self._current_bucket = value
    
    def list_buckets(self) -> list[Bucket]:
        """
        List buckets in the storage.
        
        Returns:
            list[Bucket]
        """
        return self.client.list_buckets()

    def get_object(self, object_name: str, bucket_name: Optional[str] = None) -> io.BytesIO:
        """
        Get an object from the storage.
        
        Args:
            object_name: The name of the object to get.
            bucket_name: The name of the bucket to get the object from.
        
        Returns:
            io.BytesIO
        """
        if bucket_name is None:
            if self.current_bucket is None:
                raise ValueError(f'Bucket name must be provided or current bucket must be set')
            bucket_name = self.current_bucket
        try:
            response = self.client.get_object(bucket_name, object_name)
            return bytes_to_io(response.data)
        except Exception as e:
            raise Exception(f'Failed to get object {object_name} from bucket {bucket_name}: {e}') from e
        finally:
            if response:
                response.close()
                response.release_conn()
    
    def list_objects(self, prefix: str = "", bucket_name: Optional[str] = None) -> list[Object]:
        """
        List objects in the storage.
        
        Args:
            prefix: The prefix to list the objects.
            bucket_name: The name of the bucket to list the objects from.
        
        Returns:
            list[Object]
        """
        if bucket_name is None:
            if self.current_bucket is None:
                raise ValueError(f'Bucket name must be provided or current bucket must be set')
            bucket_name = self.current_bucket
        return self.client.list_objects(bucket_name, prefix)

    def put_object(self, object_name: str, file_path: str, bucket_name: Optional[str] = None) -> ObjectWriteResult:
        """
        Put an object in the storage.
        
        Args:
            object_name: The name of the object to put.
            file_path: The file path to put the object in the storage.
            bucket_name: The name of the bucket to put the object in.
        
        Returns:
            ObjectWriteResult
        """
        if bucket_name is None:
            if self.current_bucket is None:
                raise ValueError(f'Bucket name must be provided or current bucket must be set')
            bucket_name = self.current_bucket
        return self.client.fput_object(bucket_name, object_name, file_path)

    def remove_object(self, object_name: str, bucket_name: Optional[str] = None) -> None:
        """
        Remove an object from the storage.
        
        Args:
            object_name: The name of the object to remove.
            bucket_name: The name of the bucket to remove the object from.
        
        Returns:
            None
        """
        if bucket_name is None:
            if self.current_bucket is None:
                raise ValueError(f'Bucket name must be provided or current bucket must be set')
            bucket_name = self.current_bucket
        return self.client.remove_object(bucket_name, object_name)
    
    def remove_bucket(self, bucket_name: Optional[str] = None) -> None:
        """
        Remove a bucket from the storage.
        
        Args:
            bucket_name: The name of the bucket to remove.
        
        Returns:
            None
        """
        if bucket_name is None:
            if self.current_bucket is None:
                raise ValueError(f'Bucket name must be provided or current bucket must be set')
            bucket_name = self.current_bucket
        return self.client.remove_bucket(bucket_name)
    
    def make_bucket(self, bucket_name: Optional[str] = None) -> None:
        """
        Make a bucket in the storage.
        
        Args:
            bucket_name: The name of the bucket to make.
        
        Returns:
            None
        """
        if bucket_name is None:
            if self.current_bucket is None:
                raise ValueError(f'Bucket name must be provided or current bucket must be set')
            bucket_name = self.current_bucket
        return self.client.make_bucket(bucket_name)
    
    @overload
    def __getitem__(self, key: str) -> io.BytesIO: ...
    @overload
    def __getitem__(self, key: tuple[str, str]) -> io.BytesIO: ...
    @overload
    def __getitem__(self, key: tuple[str, list[str]]) -> list[io.BytesIO]: ...
    @overload
    def __getitem__(self, key: list[tuple[str, str]]) -> list[io.BytesIO]: ...

    def __getitem__(
        self, 
        key: str | tuple[str, str] | tuple[str, list[str]] | list[tuple[str, str]] | list[str]
    ) -> io.BytesIO | list[io.BytesIO]:
        """
        Get objects from the storage.
        
        Args:
            key: The key to get the object from the storage.
            - str: The object name to get.
            - tuple[str, str]: The object name and bucket name to get.
            - tuple[str, list[str]]: The object name and list of bucket names to get.
            - list[tuple[str, str]]: The list of object names and bucket names to get.
            - list[str]: The list of object names to get.
        
        Returns:
            bytes | list[bytes]
        """
        if isinstance(key, tuple):
            if isinstance(key[1], list):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.get_object, key[0], k) for k in key[1]]
                    return [f.result() for f in concurrent.futures.as_completed(futures)]
            else:
                return self.get_object(key[0], key[1])
        elif isinstance(key, str):
            return self.get_object(key)
        elif isinstance(key, list):
            if isinstance(key[0], tuple):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.get_object, k[0], k[1]) for k in key]
                    return [f.result() for f in concurrent.futures.as_completed(futures)]
            else:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.get_object, k) for k in key]
                    return [f.result() for f in concurrent.futures.as_completed(futures)]
        else:
            raise ValueError(f'Invalid key type: {type(key)}')
        
    @overload
    def __setitem__(self, key: str, file_path: str | io.BytesIO) -> ObjectWriteResult: ...
    @overload
    def __setitem__(self, key: tuple[str, str], file_path: str | io.BytesIO) -> ObjectWriteResult: ...
    @overload
    def __setitem__(self, key: tuple[str, list[str]], file_path: str | io.BytesIO) -> list[ObjectWriteResult]: ...
    @overload
    def __setitem__(self, key: list[tuple[str, str]], file_path: str | io.BytesIO) -> list[ObjectWriteResult]: ...
    
    def __setitem__(
        self, 
        key: str | tuple[str, str] | tuple[str, list[str]] | list[tuple[str, str]] | list[str], 
        file_path: str | io.BytesIO
    ) -> ObjectWriteResult | list[ObjectWriteResult]:
        """
        Set objects in the storage.
        
        Args:
            key: The key to set the object in the storage.
            - str: The object name to set.
            - tuple[str, str]: The object name and bucket name to set.
            - tuple[str, list[str]]: The object name and list of bucket names to set.
            - list[tuple[str, str]]: The list of object names and bucket names to set.
            - list[str]: The list of object names to set.
            file_path: The file path to set the object in the storage.
        
        Returns:
            ObjectWriteResult | list[ObjectWriteResult]
        """
        if isinstance(key, tuple):
            if isinstance(key[1], list):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.put_object, key[0], k, file_path) for k in key[1]]
                    return [f.result() for f in concurrent.futures.as_completed(futures)]
            else:
                self.put_object(key[0], key[1], file_path)
        elif isinstance(key, str):
            self.put_object(key, file_path)
        elif isinstance(key, list):
            if isinstance(key[0], tuple):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.put_object, k[0], k[1], file_path) for k in key]
                    return [f.result() for f in concurrent.futures.as_completed(futures)]
            else:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.put_object, k, file_path) for k in key]
                    return [f.result() for f in concurrent.futures.as_completed(futures)]
        else:
            raise ValueError(f'Invalid key type: {type(key)}')
        

    @overload
    def __delitem__(self, key: str) -> None: ...
    @overload
    def __delitem__(self, key: tuple[str, str]) -> None: ...
    @overload
    def __delitem__(self, key: tuple[str, list[str]]) -> None: ...
    @overload
    def __delitem__(self, key: list[tuple[str, str]]) -> None: ...
        
    def __delitem__(
        self, 
        key: str | tuple[str, str] | tuple[str, list[str]] | list[tuple[str, str]] | list[str]
    ) -> None:
        """
        Delete objects from the storage.
        
        Args:
            key: The key to delete the object from the storage.
            - str: The object name to delete.
            - tuple[str, str]: The object name and bucket name to delete.
            - tuple[str, list[str]]: The object name and list of bucket names to delete.
            - list[tuple[str, str]]: The list of object names and bucket names to delete.
            - list[str]: The list of object names to delete.
        
        Returns:
            None
        """
        if isinstance(key, tuple):
            if isinstance(key[1], list):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.remove_object, key[0], k) for k in key[1]]
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
            else:
                self.remove_object(key[0], key[1])
        elif isinstance(key, str):
            self.remove_object(key)
        elif isinstance(key, list):
            if isinstance(key[0], tuple):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.remove_object, k[0], k[1]) for k in key]
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
            else:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.remove_object, k) for k in key]
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
        else:
            raise ValueError(f'Invalid key type: {type(key)}')
