require 'csv'
require 'spreadsheet'

namespace :migracao do

  desc "Migrar alunos do Filipe - MBA"
  task :turma,[:arquivo] do |t,args|
    config   = Rails.configuration.database_configuration
    host = config[Rails.env]["host"]
    user = config[Rails.env]["username"]
    password = config[Rails.env]["password"]
    database = config[Rails.env]["database"]
    client = Mysql2::Client.new(:host => host, :username => user, :password => password, :database => database)

    arq2 = CSV.open(args.arquivo,"rb",:encoding => "iso-8859-1:UTF-8",:col_sep => ",", :headers => true, :return_headers => false)
    vagas = 0
    arq2.each do |r|
      vagas = vagas + 1
    end

    codUnidade = 1
    codSerie = 7
    codTurno = 4
    anoletivo = 2013
    nome = 'Pessoas 16 Rio'
    abreviacao = 'Pessoas16Rio'
    dataInicial = '2013-10-10'
    dataFinal =   '2014-10-10'
    turno = 'null'
    datafechamento = 'null'
    horainicial = 'null'
    horafinal = 'null'
    observacoes = 'null'
    status = 'Fechada'
    client.query("insert into turma(codunidade,codserie,codturno,anoletivo,nome,abreviacao,datainicial,datafinal,
turno,datafechamento,horainicial,horafinal,observacoes,qtdvagas,status) values('#{codUnidade}','#{codSerie}','#{codTurno}',
'#{anoletivo}','#{nome}','#{abreviacao}','#{dataInicial}','#{dataFinal}',#{turno},#{datafechamento},#{horainicial},
#{horafinal},#{observacoes},#{vagas},'#{status}')")
  end

  task :alunos,[:arquivo] do |t,args|
    config   = Rails.configuration.database_configuration
    host = config[Rails.env]["host"]
    user = config[Rails.env]["username"]
    password = config[Rails.env]["password"]
    database = config[Rails.env]["database"]
    client = Mysql2::Client.new(:host => host, :username => user, :password => password, :database => database)
    begin
    arq = "saida-#{Time.now.to_date}_alunos.txt"
    relatorio = File.open(arq, 'w')
    dataValida = Proc.new{|data| true if data.to_date rescue false}
    arq2 = CSV.open(args.arquivo,"rb",:encoding => "iso-8859-1:UTF-8",:col_sep => ",", :headers => true, :return_headers => false)

    arq2.each do |r|
      if !client.query("select cpf from aluno where cpf = '#{r["CPF"]}'").any?
        p "ERRO: Aluno #{r["NOME"]} não existe ainda na base"
        nome = r["NOME"]
        dtnascimento = r["NASCIMENTO"].to_date.to_s if (!r["NASCIMENTO"].blank? && dataValida.call(r["NASCIMENTO"]))
        nacionalidade = r["NACIONALIDADE"]
        estadocivil = r["ESTADO CIVIL"]
        telefoneresid = r["TELEFONE RESID."]
        celular = r["TELEFONE CEL."]
        email = r["E-MAIL"]
        logradouro = r["ENDEREÇO"].split(',')[0] if !r["ENDEREÇO"].blank?
        complemento = r["ENDEREÇO"].split(',')[1] if (!r["ENDEREÇO"].blank? && !r["ENDEREÇO"].split(',')[1].nil?)
        numlogradouro = r["ENDEREÇO"].split(',')[1].split('-')[0].strip if (!r["ENDEREÇO"].blank? && !r["ENDEREÇO"].split(',')[1].nil?)
        complemento = r["ENDEREÇO"].split(',')[1].split('-')[1].strip if (!r["ENDEREÇO"].blank? && !r["ENDEREÇO"].split(',')[1].nil? && !r["ENDEREÇO"].split(',')[1].split('-')[1].nil?)
        codmunicipio = (r["CIDADE"].blank? || client.query("SELECT m.codigo FROM municipio m where nome = '#{r["CIDADE"]}'").first.nil?) ? '3303302' : client.query("SELECT m.codigo FROM municipio m where nome = '#{r["CIDADE"]}'").first["codigo"]
        codbairroaux = client.query("SELECT b.codigo FROM bairro b inner join municipio m on m.codigo = b.CodMunicipio and b.nome = '#{r["BAIRRO"]}' and m.codigo = #{codmunicipio}").first if !r["CIDADE"].blank?
        codbairro = codbairro.blank? ? 'null' : codbairroaux
        cep = r["CEP"]
        rg = r["RG"]
        rguf = r["UF"]
        rgdata = 'null'
        dataexp = 'null'
        orgao = r["EXP"]
        cpf = r["CPF"].to_s unless r["CPF"].blank?
        obs = 'null'

        codescolaaux = client.query("SELECT e.codigo FROM escola e where nome = '#{r["INSTITUIÇÃO"]}'").first["codigo"] if client.query("SELECT e.codigo FROM escola e where nome = '#{r["INSTITUIÇÃO"]}'").any?
        codescola = codescolaaux.blank? ? '21' : codescolaaux
        empresa = r["ORGANIZAÇÃO"]
        nomemae = r["MÃE"]
        nomepai = r["PAI"]
        client.query("insert into aluno(nome,datacadastro,DataNascimento, CodMunicipioNascimento, MunicipioNascimentoEstrangeiro,
Nacionalidade, Profissao, Cargo, Sexo, EstadoCivil, Cadeira, Telefone, TelProprio, Celular, Email, EmailProfissional, Logradouro,
NumLogradouro, Complemento, CodBairro, CodMunicipio, CEP, RG, UFDocumento, DataExpedicaoDocumento, DataExpedicao, OrgaoExpedidor,
CPF, Observacoes, CodEscola, Empresa, NomeDaMae,NomeDoPai) values('#{nome}','2014-05-05','#{dtnascimento}',null,null,
'#{nacionalidade}',null,null,'#{r["SEXO"]}','#{estadocivil}',null,'#{telefoneresid}',null,'#{celular}','#{email}',null,
'#{logradouro}',
'#{numlogradouro}','#{complemento}',#{codbairro},'#{codmunicipio}','#{cep}','#{rg}','#{rguf}',#{rgdata},#{dataexp},'#{orgao}',
'#{cpf}',#{obs},'#{codescola}','#{empresa}','#{nomemae}','#{nomepai}')")

      relatorio.puts "Aluno #{r["NOME"]} incluído na base com sucesso."

      else
        p "Aluno #{r["NOME"]} já existe na base"
        relatorio.puts "Aluno #{r["NOME"]} já existe na base"
      end
    end
    rescue => msg
      p msg.backtrace
    ensure
      relatorio.close
      arq2.close
    end
  end

  task :aluno_turma,[:arquivo] do |t,args|
    config   = Rails.configuration.database_configuration
    host = config[Rails.env]["host"]
    user = config[Rails.env]["username"]
    password = config[Rails.env]["password"]
    database = config[Rails.env]["database"]
    raise "Um arquivo de entrada deve ser informado" if args.arquivo.nil?
    client = Mysql2::Client.new(:host => host, :username => user, :password => password, :database => database)
    begin
      arq = "saida-#{Time.now.to_date}_aluno_turma.txt"
      relatorio = File.open(arq, 'w')
      codturma = 25 #A definir
      codcedente = 1
      datacadastro = Time.now.to_date.to_s
      datamatricula = Time.now.to_date.to_s
      situacao = 1

      arq2 = CSV.open(args.arquivo,"rb",:encoding => "iso-8859-1:UTF-8",:col_sep => ",", :headers => true, :return_headers => false)
      arq2.each do |r|

      #Tenho que verificar se o aluno existe na base e depois inserir ele no aluno_turma
        #Verificar, tambem, em producao, se é necessário apenas incluir nestas 3 tabelas
        mataluno = client.query("select matricula from aluno where cpf = '#{r["CPF"]}' ").first["matricula"]
        alunoJaExiste = client.query("select mataluno from aluno_turma where mataluno = #{mataluno} and codturma = #{codturma}").first["mataluno"] if !client.query("select mataluno from aluno_turma where mataluno = #{mataluno} and codturma = #{codturma}").first.nil?
        if (!mataluno.nil? && alunoJaExiste.nil?)

          p "Aluno #{r["NOME"]} não existe na turma. Inserindo... "
          relatorio.puts "Aluno #{r["NOME"]} não existe na turma. Inserindo...."
        client.query("insert into aluno_turma(MatAluno, CodTurma, CodCedente, DataCadastro, DataMatricula, Situacao) values
        ('#{mataluno}','#{codturma}','#{codcedente}','#{datacadastro}','#{datamatricula}','#{situacao}')")
        else
          p "Aluno #{r["NOME"]} já existe na turma"
          relatorio.puts "Aluno #{r["NOME"]} já existe na turma"
        end



      end
    rescue => msg
      p msg.backtrace
    ensure
      relatorio.close
      arq2.close
    end
  end

end