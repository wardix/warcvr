import Fastify, { FastifyRequest, FastifyReply } from 'fastify'
import fastifyAmqp from 'fastify-amqp'
import { config } from 'dotenv'

config()

const PORT = process.env.PORT! || 3000
const API_KEYS = process.env.API_KEYS! || '[]'
const PROTECTED_ROUTES = process.env.PROTECTED_ROUTES! || '[]'
const AMQP_URL = process.env.AMQP_URL! || 'amqp://localhost'
const JOB_EXCHANGE = process.env.JOB_EXCHANGE!
const JOB_ROUTING_KEY = process.env.JOB_ROUTING_KEY!
const JOB_QUEUE = process.env.JOB_QUEUE!
const BODY_LIMIT = process.env.BODY_LIMIT || 1048576

const apiKeys = JSON.parse(API_KEYS)
const protectedRoutes = JSON.parse(PROTECTED_ROUTES)

const validateApiKey = async (request: FastifyRequest, reply: FastifyReply) => {
  const apiKey = request.headers['x-api-key']
  if (!apiKey || !apiKeys.includes(apiKey)) {
    reply.code(401).send({ error: 'Unauthorized: Invalid API Key' })
  }
}

const fastify = Fastify({
  logger: true,
  bodyLimit: +BODY_LIMIT,
})

fastify.addHook('preValidation', async (request, reply) => {
  for (const route of protectedRoutes) {
    if (!request.routeOptions.url.startsWith(route)) {
      continue
    }
    await validateApiKey(request, reply)
    break
  }
})

fastify.register(fastifyAmqp, { url: AMQP_URL })

fastify.post('/v1/jobs', async (request: any, reply) => {
  const channel = fastify.amqp.channel

  channel.assertExchange(JOB_EXCHANGE, 'direct')
  channel.assertQueue(JOB_QUEUE, { durable: true })
  channel.bindQueue(JOB_QUEUE, JOB_EXCHANGE, JOB_ROUTING_KEY)

  channel.publish(
    JOB_EXCHANGE,
    JOB_ROUTING_KEY,
    Buffer.from(JSON.stringify(request.body)),
    { persistent: true },
  )
  reply.send({ status: 'OK' })
})

fastify.get('/', async (_request, reply) => {
  reply.send({ status: 'OK' })
})

fastify.listen({ port: +PORT, host: '0.0.0.0' }, (err) => {
  if (err) throw err
})
