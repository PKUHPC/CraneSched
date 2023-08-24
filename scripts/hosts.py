with open('/etc/hosts', 'a') as f:
    for i in range(0, 256):
        for j in range(1, 251):
            f.write("10.0." + str(i) + "." + str(j) + "\th" + str(i * 250 + j) + "\n")
