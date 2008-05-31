
dest := async-observer.rubyforge.org:/var/www/gforge-projects/async-observer/.

pushweb:
	scp -r index.html style.css $(dest)
