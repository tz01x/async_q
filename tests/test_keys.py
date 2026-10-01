import os
import re
import socket

from redis.crc import key_slot

from async_q.utils import (
    get_redis_q_key, get_redis_q_backup_key, get_retry_zset_key, get_dead_letter_key,
    get_heartbeat_key, get_workers_key, get_legacy_keys, new_worker_id,
)


def test_queue_keys_are_hash_tagged():
    assert get_redis_q_key("mail") == "async_task:{mail}"
    assert get_redis_q_backup_key("mail", "w1") == "async_task_backup:{mail}:w1"
    assert get_retry_zset_key("mail") == "async_task_retry:{mail}"
    assert get_dead_letter_key("mail") == "async_task_dead:{mail}"
    assert get_heartbeat_key("mail", "w1") == "async_task_hb:{mail}:w1"
    assert get_workers_key("mail") == "async_task_workers:{mail}"


def test_all_queue_keys_share_one_cluster_slot():
    keys = [get_redis_q_key("mail"), get_redis_q_backup_key("mail", "w1"), get_retry_zset_key("mail"),
            get_dead_letter_key("mail"), get_heartbeat_key("mail", "w1"), get_workers_key("mail")]
    assert len({key_slot(k.encode()) for k in keys}) == 1


def test_legacy_keys_match_pre_0_3_names():
    assert get_legacy_keys("mail") == {
        "main": "async_task:mail",
        "backup": "async_task_backup:mail",
        "retry": "async_task_retry:mail",
        "dead": "async_task_dead:mail",
    }


def test_worker_id_is_readable_and_unique():
    a, b = new_worker_id(), new_worker_id()
    assert a != b
    assert re.fullmatch(re.escape(socket.gethostname()) + "-" + str(os.getpid()) + "-[0-9a-f]{6}", a)
