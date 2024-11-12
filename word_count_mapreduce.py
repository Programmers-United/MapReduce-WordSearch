import threading
import os
import re  # Importando o módulo para trabalhar com expressões regulares

# Criar um lock para controle de acesso ao arquivo temporário
lock = threading.Lock()

# Função Map que gera arquivos temporários
def map_function(file_part, pattern, output_dir):
    """Função Map que busca linhas correspondentes ao padrão no arquivo"""
    
    # Compilar o padrão de expressão regular
    regex = re.compile(pattern)
    
    # Abrir a parte do arquivo e procurar o padrão
    with open(file_part, 'r') as f:
        for line in f:
            if regex.search(line):  # Usando expressão regular para procurar
                # Utilizar o lock para garantir que uma thread por vez escreva no arquivo
                with lock:
                    with open(os.path.join(output_dir, 'arq.tmp'), 'a') as f_out:
                        f_out.write(line)

# Função Reduce que lê os arquivos temporários e consolida as linhas
def reduce_function(output_dir):
    """Função Reduce que consolida as linhas correspondentes em um arquivo final"""
    with open(os.path.join(output_dir, 'arqfinal.txt'), 'w') as arq_final:
        temp_file_path = os.path.join(output_dir, 'arq.tmp')
        if os.path.exists(temp_file_path):
            with open(temp_file_path, 'r') as tmp:
                for line in tmp:
                    arq_final.write(line)  # Escreve cada linha no arquivo final

# Controlador que gerencia as threads de Map e Reduce
def controller(input_files, pattern, output_dir):
    map_threads = []

    # Criar diretório para arquivos temporários, se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Remover o arquivo temporário, caso já exista
    temp_file_path = os.path.join(output_dir, 'arq.tmp')
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

    # Executa a função Map em threads separadas
    for file_part in input_files:
        t = threading.Thread(target=map_function, args=(file_part, pattern, output_dir))
        map_threads.append(t)
        t.start()

    # Espera todas as threads Map terminarem
    for t in map_threads:
        t.join()

    # Executa a função Reduce para consolidar o resultado final
    reduce_function(output_dir)

# Exemplo de uso
if __name__ == "__main__":
    # Diretório onde os arquivos de teste estão localizados
    test_files_dir = 'test_files'
    output_dir = 'temp_dir'

    # Padrão de busca como expressão regular
    #pattern = r"^\s*\w+@\w+\.\w+"
    pattern = r"^\s*(\d{3}\.){2}\d{3}-\d\d"

    # Obter os arquivos de entrada
    input_files = [os.path.join(test_files_dir, f) for f in os.listdir(test_files_dir) if f.endswith('.txt')]
    print("Arquivos de entrada:", input_files)

    # Executar o controlador MapReduce com os arquivos gerados
    controller(input_files, pattern, output_dir)
