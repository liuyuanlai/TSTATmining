import numpy as np
import os
import matplotlib.pyplot as plt

s2c_unique_bytes = np.array([])
rootdir = "/Users/lyl/Downloads/storage"
for subdir, dirs, files in os.walk(rootdir):
	for file in files:
		try:
			if file == "log_tcp_complete":
				path = subdir + '/' + file
				print "loading " + path
				data = np.genfromtxt(path, delimiter=" ", names=True, dtype=None)
				s2c_unique_bytes = np.concatenate((s2c_unique_bytes, data['s_bytes_uniq21']))
		except:
			pass
# print len(durat_data)
# values, base = np.histogram(durat_data, bins="auto", density=True)
# cumulative = np.cumsum(values)
# cumulative = cumulative / cumulative[-1] * 100
# plt.plot(base[:-1], cumulative, c='blue')
# plt.show()

print np.max(s2c_unique_bytes)

s2c_bin = [0 for i in range(41)]

for i in s2c_unique_bytes:
    if i == 0:
        s2c_bin[0] += 1
    else:
        s2c_bin[int(np.log2(i))+1] += 1

cum_s2c_bin = np.cumsum(s2c_bin)
cum_s2c_bin = cum_s2c_bin.astype(float)
cum_s2c_bin = cum_s2c_bin / cum_s2c_bin[-1] * 100

x = [2**i for i in range(41)]
plt.xscale('log', basex=2)
plt.plot(x, cum_s2c_bin, marker='o', color='darkgray', lw=2)
plt.xlabel("TCP s2c payload ($log_2$ scale, bytes)")
plt.ylabel("Cumulative probability (%)")
plt.grid(axis='y', linestyle='--')
plt.show()