mount_name=$(sudo lsblk | awk '
NR > 1 && $1 ~ /^nvme/ && $7 == "" {
    # Convert SIZE column to bytes for comparison
    size = $4;
    unit = substr(size, length(size));
    value = substr(size, 1, length(size)-1);
    if (unit == "G") { value *= 1024^3; }
    else if (unit == "T") { value *= 1024^4; }
    else if (unit == "M") { value *= 1024^2; }
    else if (unit == "K") { value *= 1024; }
    else { value *= 1; }

    # Keep track of the largest size
    if (value > max) {
        max = value;
        largest = $1;
    }
}
END { if (largest) print largest; else print "No match found"; }
')

sudo mkfs -t xfs /dev/$mount_name

sudo rm -rf $HOME/benchmark_mount
sudo mkdir $HOME/benchmark_mount
sudo mount /dev/$mount_name $HOME/benchmark_mount

# make clone of repo on mount
sudo mkdir $HOME/benchmark_mount/IcebergEngineBenchmarking
sudo chown -R ubuntu:ubuntu $HOME/benchmark_mount


git clone https://github.com/Tmonster/IcebergEngineBenchmarking.git $HOME/benchmark_mount/IcebergEngineBenchmarking
cd $HOME/benchmark_mount/IcebergEngineBenchmarking