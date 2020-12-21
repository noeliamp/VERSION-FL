echo Enter the configuration:
read configuration
echo Configuration chosen: $configuration
echo Enter number of simulations:
read numsimul
echo Number of simulations: $numsimul
for i in $(seq 1 1 $numsimul)
do
	echo $i
	python main.py $configuration $i
done
