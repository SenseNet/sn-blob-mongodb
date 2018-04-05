$srcPath = [System.IO.Path]::GetFullPath(($PSScriptRoot + '\..\..\src'))

# delete existing packages
Remove-Item $PSScriptRoot\*.nupkg

nuget pack $srcPath\MongoDbBlobStorage\MongoDbBlobStorage.nuspec -properties Configuration=Release -OutputDirectory $PSScriptRoot