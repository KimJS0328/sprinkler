apt update && apt install -y curl \
&& curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash -o ~/.git-completion.bash \
&& echo "source ~/.git-completion.bash" >> ~/.bashrc