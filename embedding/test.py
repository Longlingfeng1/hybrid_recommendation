def print_em():
    data = open("feature_embedding.txt")
    for line in data:

        every = line.strip().split(",")
        label = every[0]
        feature = every[1]
        f = feature.strip().split(" ")
        for i in range(0,len(f)):
        #     if(float(f[i])<0.01):
        #         print("1")
        #     else:
        #         print("2")
            print(f[i])
        break



if __name__ == "__main__":
    print_em()