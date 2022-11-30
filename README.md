
# Gedis

**A golang implementation of Redis**


### Supported Features：

- _High-performance Epoll_
- _Support string, dict, list_
- _Incremental rehash_
- _Redis Serialization Protocol_
- _TTL_


## Benchmark
**Environment:**

- Go Version: go1.18
- OS: CentOS 7.6
- CPU: Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
- Memory: 4 GB

benchmark on **gedis**:
```text
[root@VM-12-17-centos ~]# redis-benchmark -c 1 -t get,set -p 8888 -q
SET: 23025.56 requests per second
GET: 24378.35 requests per second
```
benchmark on **redis**:
```text
[root@VM-12-17-centos ~]# redis-benchmark -c 1 -t get,set -q
SET: 27085.59 requests per second
GET: 27654.87 requests per second
```

**Todo** :

- [ ] AOF
- [ ] AOF rewrite