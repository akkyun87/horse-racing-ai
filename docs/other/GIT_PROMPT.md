# 1. 初期化（.gitフォルダが作成されます）

git init

# 2. ファイルをステージング（.gitignoreで指定したものは除外されます）

git add .

# 3. コミット

git commit -m "Initial commit: data pipeline structure"

# 4. ブランチ名をmainに変更

git branch -M main

# 5. GitHubのリポジトリと紐付け

# ※[URL]はGitHubで作ったリポジトリのURL（https://github.com〜.git）に置換

git remote add origin https://github.com/akkyun87/horse-racing-ai.git

# 6. アップロード実行

git push -u origin main

# Other

C:\Program Files\Git
